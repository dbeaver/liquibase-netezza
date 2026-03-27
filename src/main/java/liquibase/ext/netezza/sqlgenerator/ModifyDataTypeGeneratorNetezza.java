package liquibase.ext.netezza.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.exception.Warnings;
import liquibase.ext.netezza.database.NetezzaDatabase;
import liquibase.ext.netezza.statement.ModifyColumnDataTypeStatementNetezza;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;
import liquibase.structure.DatabaseObject;

/**
 * Netezza only allows changing length(by incrementing but not decrementing) and precision(by incrementing but not decrementing) of the data type.
 * So we need to override the default implementation of ModifyDataTypeGenerator to generate the correct SQL for Netezza.
 */
public class ModifyDataTypeGeneratorNetezza extends ModifyDataTypeGenerator {
    public ModifyDataTypeGeneratorNetezza() {
        super();
    }

    @Override
    public boolean supports(ModifyDataTypeStatement statement, Database database) {
        if (statement instanceof ModifyColumnDataTypeStatementNetezza) {
            return database instanceof NetezzaDatabase;
        } else {
            return false;
        }
    }

    @Override
    public Sql[] generateSql(ModifyDataTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String table = database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());;
        String column = database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName());
        DatabaseDataType newDataType = DataTypeFactory.getInstance().fromDescription(statement.getNewDataType(), database).toDatabaseDataType(database);
        // fallback: recreate column
        String tempColumn = column + "_TMP_" + System.currentTimeMillis();
        return new Sql[] {
            new UnparsedSql(String.format(
                "ALTER TABLE %s ADD COLUMN %s %s",
                table, tempColumn, newDataType
            ), this.getAffectedTable(statement)),
            new UnparsedSql(String.format(
                "UPDATE %s SET %s = %s",
                table, tempColumn, column
            ), this.getAffectedTable(statement)),
            new UnparsedSql(String.format(
                "ALTER TABLE %s DROP COLUMN %s RESTRICT",
                table, column
            ), this.getAffectedTable(statement)),
            new UnparsedSql(String.format(
                "ALTER TABLE %s RENAME COLUMN %s TO %s",
                table, tempColumn, newDataType
            ), this.getAffectedTable(statement))
        };

    }

    @Override
    public Warnings warn(ModifyDataTypeStatement modifyDataTypeStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Warnings().addWarning(
            "Changing data type " +
                " to " + modifyDataTypeStatement.getNewDataType() + " may cause data loss. Netezza does not support shrinking data types nor changing the type itself. Migration will attempt to recreate the column, but it may fail if there are constraints or indexes on the column."
        );
    }


    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }
}
