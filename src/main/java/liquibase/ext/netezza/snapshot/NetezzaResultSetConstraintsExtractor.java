/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2021 DBeaver Corp and others
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
package liquibase.ext.netezza.snapshot;

import java.sql.SQLException;
import java.util.List;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.NetezzaResultSetCache;
import liquibase.snapshot.NetezzaResultSetCache.SingleResultSetExtractor;
import liquibase.structure.core.Schema;

public class NetezzaResultSetConstraintsExtractor extends SingleResultSetExtractor {

    private DatabaseSnapshot databaseSnapshot;
    private Database database;
    private String catalogName;
    private String schemaName;
    private String tableName;

    public NetezzaResultSetConstraintsExtractor(DatabaseSnapshot databaseSnapshot, String catalogName, String schemaName,
                                                  String tableName) {
        super(databaseSnapshot.getDatabase());
        this.databaseSnapshot = databaseSnapshot;
        this.database = databaseSnapshot.getDatabase();
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public boolean bulkContainsSchema(String schemaKey) {
        return false;
    }

    @Override
    public NetezzaResultSetCache.RowData rowKeyParameters(CachedRow row) {
        return new NetezzaResultSetCache.RowData(this.catalogName, this.schemaName, this.database,
                row.getString("TABLE_NAME"));
    }

    @Override
    public NetezzaResultSetCache.RowData wantedKeyParameters() {
        return new NetezzaResultSetCache.RowData(this.catalogName, this.schemaName, this.database, this.tableName);
    }

    @Override
    public List<CachedRow> fastFetchQuery() throws SQLException, DatabaseException {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(this.catalogName, this.schemaName)
                .customize(this.database);

        return executeAndExtract(
                createSql(((AbstractJdbcDatabase) this.database).getJdbcCatalogName(catalogAndSchema),
                        ((AbstractJdbcDatabase) this.database).getJdbcSchemaName(catalogAndSchema), this.tableName),
                this.database, false);
    }

    @Override
    public List<CachedRow> bulkFetchQuery() throws SQLException, DatabaseException {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(this.catalogName, this.schemaName)
                .customize(this.database);

        return executeAndExtract(
                createSql(((AbstractJdbcDatabase) this.database).getJdbcCatalogName(catalogAndSchema),
                        ((AbstractJdbcDatabase) this.database).getJdbcSchemaName(catalogAndSchema), null),
                this.database);
    }

    private String createSql(String catalog, String schema, String table) {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(catalog, schema).customize(this.database);

        String jdbcSchemaName = this.database.correctObjectName(
                ((AbstractJdbcDatabase) this.database).getJdbcSchemaName(catalogAndSchema), Schema.class);

        String sql = "SELECT CONSTRAINTNAME AS CONSTRAINT_NAME, CONTYPE AS CONSTRAINT_TYPE, RELATION AS TABLE_NAME FROM "
                + catalog + ".DEFINITION_SCHEMA._V_RELATION_KEYDATA " + "where SCHEMA='" + jdbcSchemaName
                + "' and CONTYPE='u'";
        if (table != null) {
            sql += " and RELATION='" + table + "'";
        }

        return sql;
    }

}