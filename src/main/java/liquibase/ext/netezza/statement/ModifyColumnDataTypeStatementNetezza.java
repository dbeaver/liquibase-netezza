package liquibase.ext.netezza.statement;

import liquibase.statement.core.ModifyDataTypeStatement;

public class ModifyColumnDataTypeStatementNetezza extends ModifyDataTypeStatement {
    public ModifyColumnDataTypeStatementNetezza(
        String catalogName,
        String schemaName,
        String tableName,
        String columnName,
        String newDataType
    ) {
        super(catalogName, schemaName, tableName, columnName, newDataType);
    }
}
