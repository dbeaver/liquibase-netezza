/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package liquibase.ext.netezza.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.netezza.database.NetezzaDatabase;
import liquibase.ext.netezza.statement.ModifyColumnDataTypeStatementNetezza;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;

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
        DatabaseDataType type = DataTypeFactory.getInstance().fromDescription(statement.getNewDataType(), database).toDatabaseDataType(database);

        String sql = String.format(
            "ALTER TABLE %s MODIFY COLUMN (%s %s)",
            table,
            column,
            type
        );

        return new Sql[] {new UnparsedSql(sql, this.getAffectedTable(statement))};
    }
}