/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2027 DBeaver Corp and others
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
package liquibase.ext.netezza.change;

import liquibase.change.DatabaseChange;
import liquibase.change.core.ModifyDataTypeChange;
import liquibase.database.Database;
import liquibase.ext.netezza.statement.ModifyColumnDataTypeStatementNetezza;
import liquibase.statement.SqlStatement;

/**
 * Netezza only allows changing length(by incrementing but not decrementing) and precision(by incrementing but not decrementing) of the data type.
 * So we need to override the default implementation of ModifyDataTypeChange to generate the correct SQL
 * for Netezza. If the change is not safe, we will throw an exception to prevent the change from being executed.
 * This class is used to mark the change as destructive, to make it different from the default implementation of ModifyDataTypeChange which is not destructive.
 */
@DatabaseChange(
    name = "modifyDataTypeDestructive",
    description = "Modify the data type of a column, by recreating",
    priority = 5,
    appliesTo = {"column"}
)
public class ModifyDataTypeChangeDestructiveNetezza extends ModifyDataTypeChange {

    @Override
    public SqlStatement[] generateStatements(Database database) {
        ModifyColumnDataTypeStatementNetezza modifyDataTypeStatement = new ModifyColumnDataTypeStatementNetezza(this.getCatalogName(), this.getSchemaName(), this.getTableName(), this.getColumnName(), this.getNewDataType());
        return new SqlStatement[] {modifyDataTypeStatement};
    }

    @Override
    public String getCatalogName() {
        return super.getCatalogName();
    }

    @Override
    public String getSchemaName() {
        return super.getSchemaName();
    }

    @Override
    public String getTableName() {
        return super.getTableName();
    }

    @Override
    public String getColumnName() {
        return super.getColumnName();
    }

    @Override
    public String getNewDataType() {
        return super.getNewDataType();
    }
}
