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
