package liquibase.ext.netezza.diff.output.changelog;

import liquibase.Scope;
import liquibase.change.Change;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.Difference;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.core.ChangedColumnChangeGenerator;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.netezza.change.ModifyDataTypeChangeDestructiveNetezza;
import liquibase.ext.netezza.database.NetezzaDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.Schema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChangedColumnChangeGeneratorNetezza extends ChangedColumnChangeGenerator {

    public static final Pattern LENGTH_PATTERN = Pattern.compile("VARCHAR\\((\\d+)\\s*(?:BYTE)?\\)");


    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof NetezzaDatabase) {
            return Column.class.isAssignableFrom(objectType) ? 2 : -1;
        } else {
            return PRIORITY_NONE;
        }
    }

    @Override
    protected void handleTypeDifferences(
        Column column,
        ObjectDifferences differences,
        DiffOutputControl control,
        List<Change> changes,
        Database referenceDatabase,
        Database comparisonDatabase
    ) {
        Difference typeDifference = differences.getDifference("type");
        if (typeDifference == null) {
            return;
        }
        String table = column.getRelation().getName();
        String columnName = column.getName();
        Schema schema = column.getRelation().getSchema();
        boolean isSafe = isSafeExpansion(
            typeDifference.getComparedValue().toString(),
            typeDifference.getReferenceValue().toString()
        );
        if (!isSafe && isDistributionKey(referenceDatabase, schema, table, columnName)) {
            Scope.getCurrentScope().getLog(getClass())
                .warning(String.format("Column '%s.%s.%s' is a distribution key. Full table rebuild required.", schema, table, columnName));
            return;
        }

        if (!isSafe && isIndexed(referenceDatabase, schema, table, columnName)) {
            Scope.getCurrentScope().getLog(getClass()).warning(
                String.format(
                    "Column '%s.%s.%s' is indexed and cannot be safely modified in Netezza.",
                    schema, table, columnName
                )
            );
            return;
        }
        if (isSafe) {
             super.handleTypeDifferences(column, differences, control, changes, referenceDatabase, comparisonDatabase);
        } else {
            ModifyDataTypeChangeDestructiveNetezza modifyDataTypeChangeDestructiveNetezza = new ModifyDataTypeChangeDestructiveNetezza();
            modifyDataTypeChangeDestructiveNetezza.setSchemaName(schema.getName());
            modifyDataTypeChangeDestructiveNetezza.setTableName(table);
            modifyDataTypeChangeDestructiveNetezza.setColumnName(columnName);
            DataType referenceType = (DataType)typeDifference.getReferenceValue();
            modifyDataTypeChangeDestructiveNetezza.setNewDataType(referenceType.toString());
            changes.add(modifyDataTypeChangeDestructiveNetezza);
        }
    }

    private boolean isSafeExpansion(String oldType, String newType) {
        if (oldType == null) return false;

        oldType = oldType.toUpperCase();
        newType = newType.toUpperCase();

        // VARCHAR(n) -> VARCHAR(m) where m >= n
        if (oldType.startsWith("VARCHAR") && newType.startsWith("VARCHAR")) {
            int oldLen = extractSingleNumber(oldType);
            int newLen = extractSingleNumber(newType);
            return newLen >= oldLen;
        }

        return false;
    }

    private int extractSingleNumber(String type) {
        Matcher m = LENGTH_PATTERN.matcher(type);
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    private boolean isDistributionKey(Database referenceDatabase, Schema schema, String table, String columnName) {
        String sql =
            "SELECT 1\n"
                + "FROM _v_table_dist_map dm\n"
                + "WHERE dm.OWNER = ?\n"
                + "  AND dm.TABLENAME = ?\n"
                + "  AND dm.attname = ?";
        try (PreparedStatement ps = ((JdbcConnection) referenceDatabase.getConnection()).prepareStatement(sql)) {
            ps.setString(1, schema.getName().toUpperCase());
            ps.setString(2, table.toUpperCase());
            ps.setString(3, columnName.toUpperCase());

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }

        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to check distribution key for " + table + "." + columnName, e
            );
        }
    }

    protected boolean isIndexed(Database database, Schema schema, String table, String column) {
        String sql =
                  "SELECT 1\n"
                + "FROM DEFINITION_SCHEMA._V_RELATION_KEYDATA\n"
                + "WHERE \"SCHEMA\" = ?\n"
                + "  AND RELATION = ?\n"
                + "  AND ATTNAME = ?";

        try (PreparedStatement ps =
            ((JdbcConnection) database.getConnection()).prepareStatement(sql)) {

            ps.setString(1, schema.getName().toUpperCase());
            ps.setString(2, table.toUpperCase());
            ps.setString(3, column.toUpperCase());

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }

        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to check index usage for " + schema + "." + table + "." + column, e
            );
        }
    }
}
