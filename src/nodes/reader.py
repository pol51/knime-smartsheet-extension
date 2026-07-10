import logging

import pandas as pd
import knime.extension as knext

from nodes.smartsheet_client import resolve_token_and_region, create_client, validate_credentials

LOGGER = logging.getLogger(__name__)


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
        self.access_token, self.access_region = resolve_token_and_region()

    def configure(self, configure_context: knext.ConfigurationContext, *input):
        validate_credentials(configure_context, self.access_token)
        return None

    @staticmethod
    def _coerce_column(series: pd.Series, col_type: str) -> pd.Series:
        """Cast a column to a pandas dtype using the Smartsheet column type.

        Smartsheet already returns natively typed cell values (numbers as
        int/float, checkboxes as bool, dates as ISO strings), so we map from the
        column type rather than guessing by trial-and-error conversion.

        ``TEXT_NUMBER`` is Smartsheet's default type and can hold either text or
        numbers, so it is the only case we resolve from the values themselves:
        numeric only when every non-null value is already a real number,
        otherwise string (this keeps all-digit text like IDs or zip codes intact).
        """
        if col_type == "CHECKBOX":
            return series.astype("boolean")
        if col_type in ("DATE", "DATETIME", "ABSTRACT_DATETIME"):
            return pd.to_datetime(series, errors="coerce")
        if col_type == "TEXT_NUMBER":
            values = series.dropna()
            all_numeric = len(values) > 0 and all(
                isinstance(v, (int, float)) and not isinstance(v, bool)
                for v in values
            )
            if all_numeric:
                return pd.to_numeric(series, errors="coerce")
            return series.astype("string")
        # DURATION, CONTACT_LIST, PICKLIST, PREDECESSOR, MULTI_* — display text.
        return series.astype("string")

    def execute(self, exec_context: knext.ExecutionContext, *input):
        smart, self.access_token, self.access_region = create_client(
            exec_context, self.access_token, self.access_region
        )

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

        total_row_count = sheet.total_row_count or 0
        LOGGER.info("- {} rows to be read".format(total_row_count))

        column_names = [c.title for c in sheet.columns]
        column_types = [str(c.type) for c in sheet.columns]

        if total_row_count == 0:
            df = pd.DataFrame(columns=column_names)
        else:
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
            df.columns = column_names

        for name, col_type in zip(column_names, column_types):
            df[name] = self._coerce_column(df[name], col_type)

        if not self.sheetIsReport:
            df_sheets = pd.DataFrame([])
        else:
            df_sheets = pd.DataFrame([[s.id, s.name] for s in sheet.source_sheets])
            df_sheets.columns = ["Sheet ID", "Sheet Name"]

        return knext.Table.from_pandas(df), knext.Table.from_pandas(df_sheets)
