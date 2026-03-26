package liquibase.ext.netezza.change;

import liquibase.change.DatabaseChange;
import liquibase.change.core.ModifyDataTypeChange;
import liquibase.database.Database;
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
        return super.generateStatements(database);
    }
}
