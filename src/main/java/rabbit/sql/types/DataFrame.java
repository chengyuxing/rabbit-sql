package rabbit.sql.types;

import rabbit.common.types.DataRow;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * DataRow的数据框架
 */
public class DataFrame {
    private final String tableName;
    private final Collection<Map<String, Object>> rows;
    private boolean strict = true;
    private Ignore ignore;

    DataFrame(String tableName, Collection<Map<String, Object>> rows) {
        this.tableName = tableName;
        this.rows = rows;
    }

    public String getTableFieldsSql() {
        return "select * from " + tableName + " where 1 = 2";
    }

    public Ignore getIgnore() {
        return ignore;
    }

    public boolean isStrict() {
        return strict;
    }

    public String getTableName() {
        return tableName;
    }

    public void setIgnore(Ignore ignore) {
        this.ignore = ignore;
    }

    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    public Collection<Map<String, Object>> getRows() {
        return rows;
    }

    public static DataFrame of(String tableName, Collection<Map<String, Object>> rows) {
        return new DataFrame(tableName, rows);
    }

    public static DataFrame of(String tableName, Map<String, Object> row) {
        return of(tableName, Collections.singletonList(row));
    }

    public static DataFrame of(String tableName, DataRow row) {
        return of(tableName, row.toMap());
    }
}
