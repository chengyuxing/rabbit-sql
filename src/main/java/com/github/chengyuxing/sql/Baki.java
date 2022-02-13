package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.exceptions.SqlRuntimeException;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.types.Param;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 数据库基本操作通用接口
 */
public interface Baki {

    /**
     * 执行一条原始sql
     *
     * @param sql 原始sql
     * @return 行数据
     */
    DataRow execute(String sql);

    /**
     * 执行一条原始sql
     *
     * @param sql  原始sql
     * @param args 参数
     * @return 行数据
     */
    DataRow execute(String sql, Map<String, ?> args);

    /**
     * 批量执行非查询sql
     *
     * @param sqls 一组sql
     * @return 每条sql执行的结果
     */
    int[] batchExecute(String... sqls);

    /**
     * 插入
     *
     * @param tableName 数据对象
     * @param data      数据
     * @param strict    是否严格插入
     * @return 受影响的行数
     */
    int insert(String tableName, Collection<? extends Map<String, ?>> data, boolean strict);

    /**
     * 插入
     *
     * @param tableName 数据对象
     * @param data      数据
     * @return 受影响的行数
     */
    int insert(String tableName, Collection<? extends Map<String, ?>> data);

    /**
     * 插入
     *
     * @param tableName 数据对象
     * @param data      数据
     * @param strict    是否严格插入
     * @return 受影响的行数
     */
    int insert(String tableName, Map<String, ?> data, boolean strict);

    /**
     * 插入
     *
     * @param tableName 数据对象
     * @param data      数据
     * @return 受影响的行数
     */
    int insert(String tableName, Map<String, ?> data);

    /**
     * 快速插入
     * 具体逻辑可参考实现
     *
     * @param tableName 表名
     * @param data      数据
     * @param strict    是否严格插入
     * @return 受影响的行数
     */
    int fastInsert(String tableName, Collection<? extends Map<String, ?>> data, boolean strict);

    /**
     * 快速插入
     * 具体逻辑可参考实现
     *
     * @param tableName 表名
     * @param data      数据
     * @return 受影响的行数
     */
    int fastInsert(String tableName, Collection<? extends Map<String, ?>> data);

    /**
     * 删除
     *
     * @param tableName 表名
     * @param where     条件
     * @param arg       条件参数
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    int delete(String tableName, String where, Map<String, ?> arg);

    /**
     * 删除
     *
     * @param tableName 表名
     * @param where     条件
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    int delete(String tableName, String where);

    /**
     * 批量更新<br>
     * 具体逻辑可参考实现
     *
     * @param tableName 表名
     * @param data      数据
     * @param where     条件
     * @return 受影响的行数
     */
    int update(String tableName, Collection<? extends Map<String, ?>> data, String where);

    /**
     * 更新<br>
     * 具体逻辑可参考实现
     *
     * @param tableName 表名
     * @param data      数据
     * @param where     条件
     * @return 受影响的行数
     */
    int update(String tableName, Map<String, ?> data, String where);

    /**
     * 快速更新<br>
     * 具体逻辑可参考实现
     *
     * @param tableName 表名
     * @param args      参数
     * @param where     条件
     * @return 受影响的行数
     */
    int fastUpdate(String tableName, Collection<? extends Map<String, ?>> args, String where);

    /**
     * 流式查询
     *
     * @param sql 查询sql
     * @return 收集为流的结果集
     */
    Stream<DataRow> query(String sql);

    /**
     * 流式查询
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 收集为流的结果集
     */
    Stream<DataRow> query(String sql, Map<String, ?> args);

    /**
     * 查询
     *
     * @param sql 查询sql
     * @return 一组map类型结果
     */
    default List<Map<String, Object>> queryMaps(String sql) {
        try (Stream<DataRow> s = query(sql)) {
            return s.map(DataRow::toMap).collect(Collectors.toList());
        }
    }

    /**
     * 查询
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 一组map类型结果
     */
    default List<Map<String, Object>> queryMaps(String sql, Map<String, ?> args) {
        try (Stream<DataRow> s = query(sql, args)) {
            return s.map(DataRow::toMap).collect(Collectors.toList());
        }
    }

    /**
     * 分页查询
     *
     * @param query 查询sql
     * @param page  当前页
     * @param size  分页大小
     * @param <T>   类型参数
     * @return 分页构建器
     */
    <T> IPageable<T> query(String query, int page, int size);

    /**
     * 获取一条<br>
     *
     * @param sql 查询sql
     * @return 空或一条
     */
    Optional<DataRow> fetch(String sql);

    /**
     * 获取一条<br>
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 空或一条
     */
    Optional<DataRow> fetch(String sql, Map<String, ?> args);

    /**
     * 获取一条
     *
     * @param sql 查询sql
     * @return 一条数据
     */
    default Map<String, Object> fetchMap(String sql) {
        return fetch(sql).map(DataRow::toMap).orElseGet(HashMap::new);
    }

    /**
     * 获取一条
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 一条数据
     */
    default Map<String, Object> fetchMap(String sql, Map<String, ?> args) {
        return fetch(sql, args).map(DataRow::toMap).orElseGet(HashMap::new);
    }

    /**
     * 判断是否存在数据行
     *
     * @param sql sql
     * @return 是否存在
     */
    boolean exists(String sql);

    /**
     * 判断是否存在数据行
     *
     * @param sql  sql
     * @param args 参数
     * @return 是否存在
     */
    boolean exists(String sql, Map<String, ?> args);

    /**
     * 执行存储过程或函数
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return 一个或多个结果或无结果
     */
    DataRow call(String name, Map<String, Param> args);

    /**
     * 从内部获取一个连接对象
     *
     * @param func 函数体
     * @param <T>  结果类型参数
     * @return 执行结果
     */
    <T> T using(Function<Connection, T> func);

    /**
     * 获取当前数据库元数据信息
     *
     * @return 数据库元数据信息
     */
    DatabaseMetaData metaData();
}
