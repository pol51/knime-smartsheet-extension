import os
import logging

import knime.extension as knext
import pandas as pd
import smartsheet
from collections.abc import Callable
from typing import Dict, List, NewType

RowId: NewType = NewType("RowId", int)
ColumnId: NewType = NewType("ColumnId", int)
ColumnType: NewType = NewType("ColumnType", str)
ColumnTitle: NewType = NewType("ColumnTitle", str)
SyncRef: NewType = NewType("SyncRef", str)

LOGGER = logging.getLogger(__name__)

TOKEN_NAME = "SMARTSHEET_ACCESS_TOKEN"
REGION_NAME = "SMARTSHEET_REGION"


@knext.node(
    name="Smartsheet Writer",
    node_type=knext.NodeType.SINK,
    category="/community/smartsheet",
    icon_path="icons/icon/writer.png",
)
@knext.input_table(name="Input Data", description="Data source")
class SmartsheetWriterNode(knext.PythonNode):
    """Smartsheet Writer Node

    This node writes the input data into a Smartsheet grid which can then be used by other Smartsheet applications,
    dashboards and potentially access all other features in Smartsheet.
    Data can be appended to a blank sheet or synchronized with a populated sheet.
    Synchronization is based on the Reference (Index) column in the Writer node.

    It allows you to connect any external data source to Smartsheet, calculate complex KPIs within KNIME and much more.
    It also allows you to read from Smartsheet using the Smartsheet Reader node,
    process data and prepare KPIs and dashboards.

    The node can upload a new dataset on an empty Smartsheet grid. To do so, check the *"Clear sheet first"* option.

    # To set up the Smartsheet Writer Node, follow these steps:

    1. **Specify the Sheet ID:** Enter the target Smartsheet ID in the *"Sheet"* field.

        To find the Target Smartsheet ID in Smartsheet, you can:

        - Right-click on the sheet name and select *"Properties."*
        - Alternatively, go to *"File"* -> *"Properties"* within the sheet.
        - Copy the Sheet ID.

    2. **Define the Reference Column:** Enter the column name that contains the unique key for synchronization in the
     *"Ref column"* field.
    Note that this column is case-sensitive.

    3. **Optional: Clear Existing Data:** Check *"Clear sheet first"* if you want to empty the Smartsheet completely
     before uploading the new dataset. Caution: This will permanently delete the original data in the Smartsheet.

    4. **Optional: Append New Rows:** Check *"Add new"* (append) if you wish to add new rows in case their
     *"Ref Column"* content does not exist in the target Smartsheet.

    5. **Configure Credentials:**
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

        **Note**: If the region is unspecified, it will use by default US Smartsheet services (smartsheet.com)


    **Important Note**: Synchronization requires unique values in the *"Ref Column"* of both the dataset to be imported
     and the target Smartsheet. Presence of duplicate values in either will result in an error.

    """

    sheetId = knext.StringParameter(
        label="Sheet",
        description="The Smartsheet sheet to be written",
        default_value="",
    )
    referenceColumn = knext.StringParameter(
        label="Ref column", description="The name of the column to be used as reference"
    )
    clearFirst = knext.BoolParameter(
        label="Clear sheet first", description="Remove all rows before writing"
    )
    addMissingRefs = knext.BoolParameter(
        label="Add new", description="Add new (no match with output) references"
    )
    # removeOldRefs = knext.BoolParameter(
    #    label='Remove old', description='Remove old (no match with input) references')
    removeOldRefs = False

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

    @classmethod
    def get_smartsheet_cell_value(cls, pd_value, col_type: ColumnType):
        if pd.isna(pd_value):
            return ""

        if col_type == "CHECKBOX":
            return bool(pd_value)

        try:
            if float(int(pd_value)) == float(pd_value):
                return int(pd_value)
            else:
                return float(pd_value)
        except Exception as _e:
            return str(pd_value)

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

        input_pandas: pd.PeriodDtype = input[0].to_pandas()

        smart: smartsheet.Smartsheet = smartsheet.Smartsheet(
            self.access_token, api_base=api_base
        )
        sheet = smart.Sheets.get_sheet(self.sheetId)
        if not sheet:
            raise knext.InvalidParametersError("Output sheet not found in Smartsheet")

        if self.clearFirst:
            LOGGER.info("deleting all existing rows...")
            page_size = 300
            row_ids: List[RowId] = [r.id for r in sheet.rows]
            for ids in [
                row_ids[i : i + page_size] for i in range(0, len(row_ids), page_size)
            ]:
                smart.Sheets.delete_rows(self.sheetId, ids)
            sheet = smart.Sheets.get_sheet(self.sheetId)

        input_columns: List[ColumnTitle] = [c for c in input_pandas]
        output_columns: Dict[ColumnTitle, ColumnId] = {
            c.title: c.id for c in sheet.columns
        }
        output_columns_name_by_id: Dict[ColumnId, ColumnTitle] = {
            v: k for k, v in output_columns.items()
        }

        LOGGER.info("input: %s", repr({c: c in output_columns for c in input_columns}))
        LOGGER.info("output: %s", repr(output_columns))

        if self.referenceColumn not in input_columns:
            raise knext.InvalidParametersError(
                "Reference column not found in input columns"
            )
        if self.referenceColumn not in output_columns.keys():
            raise knext.InvalidParametersError(
                "Reference column not found in output columns"
            )

        ref_column_id: ColumnId = output_columns[self.referenceColumn]

        input_references: List[SyncRef] = [
            r for r in input_pandas[self.referenceColumn]
        ]
        LOGGER.info("input refs: %s", repr(input_references))

        output_ref_no_match: List[SyncRef] = list()
        output_ref_to_be_synced: Dict[SyncRef, RowId] = dict()
        output_data_to_be_synced: Dict[RowId, smartsheet.models.Row] = dict()
        output_ref_missing: List[SyncRef] = list()
        for row in sheet.rows:
            for cell in [c for c in row.cells if c.value is not None]:
                if cell.column_id == ref_column_id:
                    if cell.value in input_references:
                        output_ref_to_be_synced[SyncRef(cell.value)] = row.id
                        output_data_to_be_synced[row.id] = row
                    else:
                        output_ref_no_match.append(SyncRef(cell.value))
        output_ref_missing = [
            ref for ref in input_references if ref not in output_ref_to_be_synced.keys()
        ]

        LOGGER.info("sync to be done:")
        LOGGER.info("- matching refs: %d -> UPDATE", len(output_ref_to_be_synced))
        LOGGER.info(
            "- new      refs: %d -> %s",
            len(output_ref_missing),
            "CREATE" if self.addMissingRefs else "SKIP",
        )
        LOGGER.info(
            "- old      refs: %d -> %s",
            len(output_ref_no_match),
            "DELETE" if self.removeOldRefs else "SKIP",
        )

        indexed_input = input_pandas.set_index(self.referenceColumn)

        columns_type: Dict[ColumnId:ColumnType] = {c.id: c.type for c in sheet.columns}

        # sync existing rows
        updated_rows: List[smartsheet.models.Row] = []
        synced_columns = set(input_columns) - {self.referenceColumn}
        for ref, rowId in output_ref_to_be_synced.items():
            updated_row: smartsheet.models.Row = smartsheet.models.Row()
            updated_row.id = rowId
            source_row = indexed_input.loc[ref]

            target_row: smartsheet.models.Row = output_data_to_be_synced[rowId]

            for old_cell in target_row.cells:
                if output_columns_name_by_id[old_cell.column_id] in synced_columns:
                    updated_cell: smartsheet.models.Cell = smartsheet.models.Cell()
                    updated_cell.column_id = old_cell.column_id

                    value = source_row[output_columns_name_by_id[old_cell.column_id]]
                    updated_cell.value = self.get_smartsheet_cell_value(
                        value, columns_type[old_cell.column_id]
                    )

                    updated_row.cells.append(updated_cell)

            # add row to the list
            updated_rows.append(updated_row)
        if len(updated_rows) > 0:
            smart.Sheets.update_rows(self.sheetId, updated_rows)
        LOGGER.info("- {} matching rows UPDATED".format(len(updated_rows)))

        # add new rows
        if self.addMissingRefs:
            new_rows: List[smartsheet.models.Row] = []
            for ref in output_ref_missing:
                new_row: smartsheet.models.Row = smartsheet.models.Row()
                new_row.to_bottom = True
                source_row = indexed_input.loc[ref]

                for column_name, column_id in output_columns.items():
                    if column_name in input_columns:
                        new_cell: smartsheet.models.Cell = smartsheet.models.Cell()
                        new_cell.column_id = column_id

                        if column_name != self.referenceColumn:
                            value = source_row[column_name]
                        else:
                            value = source_row.name
                        new_cell.value = self.get_smartsheet_cell_value(
                            value, columns_type[column_id]
                        )

                        new_row.cells.append(new_cell)

                # add row to the list
                new_rows.append(new_row)

            if len(new_rows) > 0:
                smart.Sheets.add_rows(self.sheetId, new_rows)
            LOGGER.info("- {} new rows CREATED".format(len(new_rows)))

        return None


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
