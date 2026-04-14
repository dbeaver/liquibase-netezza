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
