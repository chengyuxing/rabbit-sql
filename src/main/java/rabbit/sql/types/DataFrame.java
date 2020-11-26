package rabbit.sql.types;

import rabbit.common.types.DataRow;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * 用来进行插入操作的数据容器
 */
public class DataFrame {
    private final String tableName;
    private final Collection<Map<String, Object>> rows;
    private boolean strict = true;
    private Ignore ignore;

    /**
     * 构造函数
     *
     * @param tableName 表名
     * @param rows      行数据
     */
    DataFrame(String tableName, Collection<Map<String, Object>> rows) {
        this.tableName = tableName;
        this.rows = rows;
    }

    /**
     * 获取表字段sql
     *
     * @return 表字段sql
     */
    public String getTableFieldsSql() {
        return "select * from " + tableName + " where 1 = 2";
    }

    /**
     * 获取忽略的值类型
     *
     * @return 忽略类型
     */
    public Ignore getIgnore() {
        return ignore;
    }

    /**
     * 是否是严格模式
     *
     * @return 是否严格模式
     */
    public boolean isStrict() {
        return strict;
    }

    /**
     * 获取表名
     *
     * @return 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置忽略插入的类型
     *
     * @param ignore 忽略
     * @return DataFrame
     */
    public DataFrame ignore(Ignore ignore) {
        this.ignore = ignore;
        return this;
    }

    /**
     * 设置严格模式，默认为严格模式<br>
     * 如果为 非严格模式 ，则在生成sql时时会忽略行数据中不存在的列名
     *
     * @param strict 是否严格模式
     * @return DataFrame
     */
    public DataFrame strict(boolean strict) {
        this.strict = strict;
        return this;
    }

    /**
     * 获取行数据
     *
     * @return 行数据
     */
    public Collection<Map<String, Object>> getRows() {
        return rows;
    }

    /**
     * 创建一个数据容器
     *
     * @param tableName 表名
     * @param rows      行数据类型集合
     * @return DataFrame
     */
    public static DataFrame of(String tableName, Collection<Map<String, Object>> rows) {
        return new DataFrame(tableName, rows);
    }

    /**
     * 创建一个数据容器
     *
     * @param tableName 表名
     * @param row       行数据类型
     * @return DataFrame
     */
    public static DataFrame of(String tableName, Map<String, Object> row) {
        return of(tableName, Collections.singletonList(row));
    }

    /**
     * 行数据类型
     *
     * @param tableName 表名
     * @param row       行数据类型
     * @return DataFrame
     */
    public static DataFrame of(String tableName, DataRow row) {
        return of(tableName, row.toMap());
    }
}
