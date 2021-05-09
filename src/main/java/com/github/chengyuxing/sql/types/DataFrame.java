package com.github.chengyuxing.sql.types;

import com.github.chengyuxing.common.DataRow;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 用来进行插入操作的数据容器
 */
public class DataFrame {
    private final String tableName;
    private final Collection<Map<String, Object>> rows;
    private boolean strict = true;

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
    public static DataFrame ofMaps(String tableName, Collection<Map<String, Object>> rows) {
        return new DataFrame(tableName, rows);
    }

    /**
     * 创建一个数据容器
     *
     * @param tableName 表名
     * @param rows      行数据类型集合
     * @return DataFrame
     */
    public static DataFrame ofRows(String tableName, Collection<DataRow> rows) {
        List<Map<String, Object>> maps = rows.stream().map(DataRow::toMap).collect(Collectors.toList());
        return ofMaps(tableName, maps);
    }

    /**
     * 创建一个数据容器
     *
     * @param tableName 表名
     * @param entities  行数据类型集合
     * @return DataFrame
     */
    public static DataFrame ofEntities(String tableName, Collection<?> entities) {
        List<DataRow> rows = entities.stream().map(DataRow::fromEntity).collect(Collectors.toList());
        return ofRows(tableName, rows);
    }

    /**
     * 创建一个数据容器
     *
     * @param tableName 表名
     * @param row       行数据类型
     * @return DataFrame
     */
    public static DataFrame ofMap(String tableName, Map<String, Object> row) {
        return ofMaps(tableName, Collections.singletonList(row));
    }

    /**
     * 创建一个数据容器
     *
     * @param tableName 表名
     * @param row       行数据类型
     * @return DataFrame
     */
    public static DataFrame ofRow(String tableName, DataRow row) {
        return ofMap(tableName, row.toMap());
    }

    /**
     * 创建一个数据容器
     *
     * @param tableName 表名
     * @param entity    实体
     * @return DataFrame
     */
    public static DataFrame ofEntity(String tableName, Object entity) {
        return ofRow(tableName, DataRow.fromEntity(entity));
    }
}
