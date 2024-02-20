import os
import logging

import pandas as pd
import knime_extension as knext
import smartsheet
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)

TOKEN_NAME = "SMARTSHEET_ACCESS_TOKEN"


@knext.node(
    name="Smartsheet Reader",
    node_type=knext.NodeType.SOURCE,
    category="/community/smartsheet",
    icon_path="icons/icon/reader.png",
)
@knext.output_table(name="Output Data", description="Data from Smartsheet")
@knext.output_table(
    name="Output Sources Sheets",
    description="Source Sheets for the Report (only for reports)",
)
class SmartsheetReaderNode(knext.PythonNode):
    """Smartsheet Reader Node

    Reads Smartsheet sheet.

    The SMARTSHEET_ACCESS_TOKEN can be set via the environment. If that is not
    set, you can set it via credentials from the flow variables, which can be
    set e.g. via Credentials Configuration, where the parameter name must be
    SMARTSHEET_ACCESS_TOKEN and the token must be in the password field.
    """

    sheetId = knext.StringParameter(
        label="ID",
        description="The Smartsheet sheet or report to be read",
        default_value="",
    )
    sheetIsReport = knext.BoolParameter(
        label="Report (Sheet otherwise)",
        description="The source ID is a report (sheet otherwise)",
        default_value=False,
    )

    def __init__(self):
        self.access_token = os.environ.get(TOKEN_NAME, "")

        column_filter: Callable[[knext.Column], bool] = None

        column = knext.ColumnParameter(
            label="Column",
            description=None,
            port_index=0,  # the port from which to source the input table
            column_filter=column_filter,  # a (lambda) function to filter columns
            include_row_key=False,  # whether to include the table Row ID column in the list of selectable columns
            include_none_column=False,  # whether to enable None as a selectable option, which returns "<none>"
            since_version=None,
        )

    def configure(self, configure_context: knext.ConfigurationContext):
        if not self.access_token:
            try:
                credentials = configure_context.get_credentials(TOKEN_NAME)
                if credentials.password == "":
                    raise KeyError
                self.access_token = credentials.password
                LOGGER.debug(
                    f"{TOKEN_NAME} has been set via credentials coming in as flow variable."
                )
            except KeyError:
                raise knext.InvalidParametersError(
                    f"Either {TOKEN_NAME} was not set in your env or \
the Credentials Configuration node (which should \
set the flow variable for this node) did not contain \
a parameter called {TOKEN_NAME} or the password in there was empty."
                )
        return None

    def execute(self, exec_context: knext.ExecutionContext):
        smart = smartsheet.Smartsheet(access_token=self.access_token)

        page_size = 1

        if not self.sheetIsReport:
            get_page = lambda page: smart.Sheets.get_sheet(
                self.sheetId, page_size=page_size, page=page
            )
        else:
            get_page = lambda page: smart.Reports.get_report(
                self.sheetId, include=["sourceSheets"], page_size=page_size, page=page
            )

        sheet = get_page(1)
        page_size = 1000

        exec_context.flow_variables.update(
            {"smartsheet_reader.source_name": sheet.name}
        )

        dfs = list()

        total_row_count = sheet.total_row_count
        LOGGER.info("- {} rows to be read".format(total_row_count))
        for current_page in [
            x + 1 for x in range(0, int((total_row_count - 1) / page_size) + 1)
        ]:
            sheet = get_page(current_page)
            dfs.append(
                pd.DataFrame(
                    [[c.value for c in r.cells] for r in sheet.rows], dtype="object"
                )
            )

        df = pd.concat(dfs, ignore_index=True)
        df.columns = [c.title for c in sheet.columns]
        for t in [c.title for c in sheet.columns]:
            try:
                df.astype({t: "float"})
            except Exception as _:
                try:
                    df.astype({t: "int64"})
                except Exception as _:
                    try:
                        df = df.astype({t: "string"})
                    except Exception as _:
                        pass

        if not self.sheetIsReport:
            df_sheets = pd.DataFrame([])
        else:
            df_sheets = pd.DataFrame([[s.id, s.name] for s in sheet.source_sheets])
            df_sheets.columns = ["Sheet ID", "Sheet Name"]

        return knext.Table.from_pandas(df), knext.Table.from_pandas(df_sheets)
