package liquibase.ext.netezza.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.Warnings;
import liquibase.ext.netezza.database.NetezzaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;

import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Netezza only allows changing length(by incrementing but not decrementing) and precision(by incrementing but not decrementing) of the data type.
 * So we need to override the default implementation of ModifyDataTypeGenerator to generate the correct SQL for Netezza.
 */
public class ModifyDataTypeGeneratorNetezza extends ModifyDataTypeGenerator {
    public ModifyDataTypeGeneratorNetezza() {
        super();
    }

    @Override
    public boolean supports(ModifyDataTypeStatement statement, Database database) {
        return database instanceof NetezzaDatabase;
    }

    @Override
    public Sql[] generateSql(ModifyDataTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String table = statement.getTableName();
        String column = statement.getColumnName();
        String newType = statement.getNewDataType();
        database.createsIndexesForForeignKeys();
//        // You *should* fetch old type from DB metadata
//        String oldType = getCurrentColumnType(database, table, column);
//        if (isSafeExpansion(oldType, newType)) {
//            // allowed case
//            String sql = String.format(
//                "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
//                table, column, newType
//            );
//            return new Sql[]{ new UnparsedSql(sql) };
//        }

        // fallback: recreate column
        String tempColumn = column + "_TMP";

        return new Sql[] {
            new UnparsedSql(String.format(
                "ALTER TABLE %s ADD COLUMN %s %s",
                table, tempColumn, newType
            )),
            new UnparsedSql(String.format(
                "UPDATE %s SET %s = %s",
                table, tempColumn, column
            )),
            new UnparsedSql(String.format(
                "ALTER TABLE %s DROP COLUMN %s",
                table, column
            )),
            new UnparsedSql(String.format(
                "ALTER TABLE %s RENAME COLUMN %s TO %s",
                table, tempColumn, column
            ))
        };

    }

//    private boolean isSafeExpansion(String oldType, String newType) {
//        if (oldType == null) return false;
//
//        oldType = oldType.toUpperCase();
//        newType = newType.toUpperCase();
//
//        // VARCHAR(n) -> VARCHAR(m) where m >= n
//        if (oldType.startsWith("VARCHAR") && newType.startsWith("VARCHAR")) {
//            int oldLen = extractSingleNumber(oldType);
//            int newLen = extractSingleNumber(newType);
//            return newLen >= oldLen;
//        }
//
//        // NUMERIC(p,s) -> NUMERIC(p2,s2) where p2 >= p and s2 >= s
//        if (oldType.startsWith("NUMERIC") && newType.startsWith("NUMERIC")) {
//            int[] oldPS = extractTwoNumbers(oldType);
//            int[] newPS = extractTwoNumbers(newType);
//            return newPS[0] >= oldPS[0] && newPS[1] >= oldPS[1];
//        }
//
//        return false;
//    }

    private int extractSingleNumber(String type) {
        Matcher m = Pattern.compile("\\((\\d+)\\)").matcher(type);
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    private int[] extractTwoNumbers(String type) {
        Matcher m = Pattern.compile("\\((\\d+),(\\d+)\\)").matcher(type);
        if (m.find()) {
            return new int[]{
                Integer.parseInt(m.group(1)),
                Integer.parseInt(m.group(2))
            };
        }
        return new int[]{0, 0};
    }

    private String getCurrentColumnType(Database database, String table, String column) {
        try {
            try (
                Statement statement = database.getConnection()
                    .getUnderlyingConnection()
                    .createStatement()
            ) {
                return statement
                    .executeQuery(
                        "SELECT attname, atttypid::regtype " +
                            "FROM _v_relation_column " +
                            "WHERE name = '" + table + "' AND attname = '" + column + "'"
                    )
                    .next() ?
                    statement
                        .executeQuery(
                            "SELECT atttypid::regtype FROM _v_relation_column " +
                                "WHERE name = '" + table + "' AND attname = '" + column + "'"
                        )
                        .getString(1)
                    : null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch column metadata", e);
        }
    }

    @Override
    public Warnings warn(ModifyDataTypeStatement modifyDataTypeStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Warnings().addWarning(
            "Changing data type from " + getCurrentColumnType(database, modifyDataTypeStatement.getTableName(), modifyDataTypeStatement.getColumnName()) +
                " to " + modifyDataTypeStatement.getNewDataType() + " may cause data loss. Netezza does not support shrinking data types nor changing the type itself. Migration will attempt to recreate the column, but it may fail if there are constraints or indexes on the column."
        );
    }



    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }
}
