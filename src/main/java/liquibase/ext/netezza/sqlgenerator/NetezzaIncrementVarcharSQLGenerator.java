package liquibase.ext.netezza.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.netezza.database.NetezzaDatabase;
import liquibase.ext.netezza.statement.ModifyColumnDataTypeStatementNetezza;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;
import liquibase.structure.DatabaseObject;

/**
 *  Netezza only allows changing length(by incrementing but not decrementing)
 *  Most of other cases are covered by ModifyDataTypeGeneratorNetezza which will recreate the column,
 *  for the case of incrementing varchar length, we can directly modify the column without recreating it,
 *  so we need this generator to handle this case.
 */
public class NetezzaIncrementVarcharSQLGenerator extends ModifyDataTypeGenerator {

    @Override
    public boolean supports(ModifyDataTypeStatement statement, Database database) {
        if (statement instanceof ModifyColumnDataTypeStatementNetezza) {
            return false;
        }
        return database instanceof NetezzaDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE; // higher than default
    }

    @Override
    public Sql[] generateSql(ModifyDataTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        String table = database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());;
        String column = database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName());
        String type = statement.getNewDataType();

        String sql = String.format(
            "ALTER TABLE \"%s\" MODIFY COLUMN (\"%s\" %s)",
            table,
            column,
            type
        );

        return new Sql[] {new UnparsedSql(sql, this.getAffectedTable(statement))};
    }
}