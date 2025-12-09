import knime.extension as knext

category = knext.category(
    path="/community",
    level_id="smartsheet",
    name="Smartsheet",
    description="Nodes for reading/writing Smartsheet data",
    icon="icons/icon/smartsheet.png",
)

import nodes.reader
import nodes.writer
