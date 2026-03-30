package liquibase.ext.netezza.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.netezza.database.NetezzaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropColumnGenerator;
import liquibase.statement.core.DropColumnStatement;
import liquibase.structure.DatabaseObject;

public class DropColumnGeneratorNetezza extends DropColumnGenerator {
    public int getPriority() {
        return 5;
    }

    public boolean supports(DropColumnStatement statement, Database database) {
        return database instanceof NetezzaDatabase;
    }

    public Sql[] generateSql(DropColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Sql[] sqls = new Sql[1];
        String tableName = database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());
        sqls[0] = new UnparsedSql(
            "ALTER TABLE " + tableName + " DROP " + database.escapeColumnName(
                statement.getCatalogName(),
                statement.getSchemaName(),
                statement.getTableName(),
                statement.getColumnName()
            ) + " RESTRICT", new DatabaseObject[0]
        );
        return sqls;
    }
}
