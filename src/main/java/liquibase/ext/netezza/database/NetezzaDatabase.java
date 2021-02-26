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
package liquibase.ext.netezza.database;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.OfflineConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;

public class NetezzaDatabase extends AbstractJdbcDatabase {

    public static final String PRODUCT_NAME = "Netezza NPS";
    private String connectionSchemaName;
    private Set<String> reservedWords = new HashSet<>();

    public NetezzaDatabase() {
    	super.setCurrentDateTimeFunction("NOW()");
        super.unquotedObjectsAreUppercased = true;
        super.sequenceNextValueFunction = "NEXT VALUE FOR %s";
        super.unmodifiableDataTypes.addAll(Arrays.asList("int", "integer", "smallint", "bigint", "bool", "boolean",
                "bit", "byteint", "float", "interval", "real", "time", "date", "timestamp", "timestamp with time zone"));
        
        reservedWords.addAll(Arrays.asList("CURRENT_DB",
                "CURRENT_SID",
                "CURRENT_USERID",
                "CURRENT_USEROID",
                "CURRENT_CATALOG",
                "CURRENT_PATH",
                "CURRENT_SCHEMA",
                "CURRENT_DATE",
                "CURRENT_TIME",
                "CURRENT_TIMESTAMP",
                "CURRENT_TX_PATH",
                "CURRENT_TX_SCHEMA",
                "CURRENT_USER",
                "DATE_PART",
                "DATE_TRUNC",
                "TIMEOFDAY",
                "ANALYZE",
                "COMMENT",
                "DECODE",
                "RESET",
                "DISTRIBUTE",
                "LOCK",
                "SHOW",
                "SYNONYM",
                "EXPRESS",
                "ONLINE",
                "RESET"));
    }

    @Override
    public String getShortName() {
        return "netezza";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return 5480;
    }


    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:netezza:")) {
            return "org.netezza.Driver";
        }
        return null;
    }

    @Override
    public String getDefaultCatalogName() {
        return super.getDefaultCatalogName() == null ? null : super.getDefaultCatalogName().toUpperCase();
    }

    @Override
    public String getDefaultSchemaName() {
        return super.getDefaultSchemaName() == null ? null : super.getDefaultSchemaName().toUpperCase();
    }

    @Override
    public String getJdbcCatalogName(final CatalogAndSchema schema) {
        return super.getJdbcCatalogName(schema) == null ? null : super.getJdbcCatalogName(schema).toUpperCase();
    }

    @Override
    public String getJdbcSchemaName(final CatalogAndSchema schema) {
        return super.getJdbcSchemaName(schema) == null ? null : super.getJdbcSchemaName(schema).toUpperCase();
    }

    @Override
    public boolean supportsCatalogs() {
        return true;
    }

    @Override
    public boolean supportsCatalogInObjectName(Class<? extends DatabaseObject> type) {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public String getDatabaseChangeLogTableName() {
        return super.getDatabaseChangeLogTableName().toUpperCase();
    }

    @Override
    public String getDatabaseChangeLogLockTableName() {
        return super.getDatabaseChangeLogLockTableName().toUpperCase();
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return true;
    }


    @Override
    public boolean supportsSchemas() {
        return true;
    }
    
    @Override
    protected String getConnectionSchemaName() {
    	if (connectionSchemaName == null) {
    		if ((getConnection() == null) || (getConnection() instanceof OfflineConnection)) {
    			return "ADMIN";
    		}
    		try {
    			String schemaName = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this).queryForObject(new RawSqlStatement("SELECT DEFSCHEMA FROM _V_DATABASE WHERE DATABASE='" + getConnection().getCatalog() + "'"), String.class);
    			if (schemaName != null) {
    				connectionSchemaName = schemaName.trim();
    			}
    		} catch (Exception e) {
    			Scope.getCurrentScope().getLog(getClass()).info("Error getting connection schema", e);
    		}
    	}
        return connectionSchemaName;
    }
}
