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
package liquibase.ext.netezza.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.NumberType;
import liquibase.ext.netezza.database.NetezzaDatabase;

public class NetezzaNumberType extends NumberType {
    public int getPriority() {
        return 5;
    }

    public boolean supports(Database database) {
        return database instanceof NetezzaDatabase;
    }

    public DatabaseDataType toDatabaseDataType(Database database) {
        return new DatabaseDataType("NUMERIC", this.getParameters());
    }
}
