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
package liquibase.snapshot;

import java.sql.SQLException;
import java.util.List;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;

public class NetezzaResultSetCache extends ResultSetCache {
    public static class RowData extends ResultSetCache.RowData {
        public RowData(String catalog, String schema, Database database, String... parameters) {
            super(catalog, schema, database, parameters);
        }
    }

    public abstract static class SingleResultSetExtractor extends ResultSetCache.SingleResultSetExtractor {
        public SingleResultSetExtractor(Database database) {
            super(database);
        }

        protected boolean shouldBulkSelect(String schemaKey, NetezzaResultSetCache resultSetCache) {
            return super.shouldBulkSelect(schemaKey, resultSetCache);
        }

        @Override
        public List<CachedRow> executeAndExtract(String sql, Database database) throws DatabaseException, SQLException {
            return super.executeAndExtract(sql, database);
        }

        @Override
        public List<CachedRow> executeAndExtract(String sql, Database database, boolean informixTrimHint)
                throws DatabaseException, SQLException {
            return super.executeAndExtract(sql, database, informixTrimHint);
        }

    }
}
