import os
import logging

import pandas as pd
import knime.extension as knext
import smartsheet
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)

TOKEN_NAME = "SMARTSHEET_ACCESS_TOKEN"
REGION_NAME = "SMARTSHEET_REGION"


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

    This node downloads data from Smartsheet for further processing and analysis within KNIME.
    It allows you to integrate Smartsheet data with other data sources, calculate KPIs, and visualize results in
     various ways, including writing data back to Smartsheet using the Smartsheet Writer node.

    # To set up the Smartsheet Reader Node, follow these steps:

    1. **Specify the ID:**  Enter the Smartsheet Sheet ID or Report ID in the *"ID"* field.

        To find the ID in Smartsheet:

        - Right-click on the sheet or report name and select *"Properties."*
        - Alternatively, go to *"File"* -> *"Properties"* within the sheet or report.
        - Copy the Sheet ID or Report ID.

    2. **Indicate Report (Optional):**
        Check the *"Report"* checkbox if you are reading data from a Smartsheet report.

    3. **Configure Credentials:**
        - **Via Knime *"Credentials Configuration"* node:**

            - Add the *"Credentials Configuration"* node to your workflow.
            - Connect the variable port of the *"Credentials Configuration"* node to the upper left variable port of
             the Smartsheet Writer node.
            - In the *"Credentials Configuration"* node:

                1. Enter *`"SMARTSHEET_ACCESS_TOKEN"`* in the *"Parameter/Variable Name"* field.
                2. Enter your Smartsheet access token in the *"Password"* field.

                **Note**: If your Smartsheet token is for European or Government Smartsheet server, you need to prefix
                your Smartsheet token with the region (*`eu`*, *`gov`*). (eg: *"eu:<SMARTSHEET_ACCESS_TOKEN>"*)

        - **Alternatively, using environment variables:**
            - Set your Smartsheet access token in *`"SMARTSHEET_ACCESS_TOKEN"`* env variable.
            - Optional: Set your Smartsheet region (*`eu`*, *`gov`*) in *`"SMARTSHEET_REGION"`* env variable.

        **Note**: if the region is unspecified, it will use by default US Smartsheet services (smartsheet.com)
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
        self.access_region = os.environ.get(REGION_NAME, "")

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

    def configure(self, configure_context: knext.ConfigurationContext, *input):
        if not self.access_token:
            _get_access_token_from_credentials_configuration(configure_context)
        return None

    def execute(self, exec_context: knext.ExecutionContext, *input):
        if not self.access_token:
            self.access_token = _get_access_token_from_credentials_configuration(
                exec_context
            )

            token_parts = self.access_token.split(":")
            if len(token_parts) == 2:
                self.access_region, self.access_token = token_parts

        if self.access_region == "eu":
            api_base = smartsheet.__eu_base__
        elif self.access_region == "gov":
            api_base = smartsheet.__gov_base__
        else:
            api_base = smartsheet.__api_base__

        smart = smartsheet.Smartsheet(access_token=self.access_token, api_base=api_base)

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


def _get_access_token_from_credentials_configuration(
    context: knext.ConfigurationContext,
):
    try:
        credentials = context.get_credentials(TOKEN_NAME)
        if credentials.password == "":
            raise KeyError
        LOGGER.debug(
            f"{TOKEN_NAME} has been set via credentials coming in as flow variable."
        )
        return credentials.password
    except KeyError:
        raise knext.InvalidParametersError(
            f"Either {TOKEN_NAME} was not set in your env or \
the Credentials Configuration node (which should \
set the flow variable for this node) did not contain \
a parameter called {TOKEN_NAME} or the password in there was empty."
        )
