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

import java.util.Locale;

import liquibase.database.Database;
import liquibase.ext.netezza.database.NetezzaDatabase;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.SequenceSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;

public class NetezzaSequenceSnapshotGenerator extends SequenceSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof NetezzaDatabase) {
        	int priority = super.getPriority(objectType, database);
            return priority += PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[] { SequenceSnapshotGenerator.class };
    }

    @Override
    protected String getSelectSequenceSql(Schema schema, Database database) {
    	if (database instanceof NetezzaDatabase) {
    		String catalogName = schema.getCatalogName().toUpperCase(Locale.ENGLISH);
    		return "SELECT\n" +
                    "S.SEQNAME AS SEQUENCE_NAME, " +
       			 	" VT.CYCLE AS WILL_CYCLE, VT.INCREMENT AS INCREMENT_BY, VT.* FROM " + catalogName + ".DEFINITION_SCHEMA._V_SEQUENCE S" +
                    "\nLEFT JOIN " + catalogName + ".DEFINITION_SCHEMA._VT_SEQUENCE VT " +
                    "ON S.OBJID = VT.SEQ_ID" +
                    "\nWHERE s.SCHEMA ='" + schema.getName() + "'";
        }
    	
        return super.getSelectSequenceSql(schema, database);
    }

}