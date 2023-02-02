package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.support.executor.InsertExecutor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.UpdateExecutor;
import com.github.chengyuxing.sql.types.Param;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
    int[] batchExecute(List<String> sqls);

    /**
     * 批量执行非查询sql
     *
     * @param sqls 一组sql
     * @return 每条sql执行的结果
     */
    default int[] batchExecute(String... sqls) {
        return batchExecute(Arrays.asList(sqls));
    }

    /**
     * 删除
     *
     * @param tableName 表名
     * @param where     条件
     * @param arg       条件参数，支持参数占位符e.g. {@code id = :id}
     * @return 受影响的行数
     */
    int delete(String tableName, String where, Map<String, ?> arg);

    /**
     * 删除
     *
     * @param tableName 表名
     * @param where     条件
     * @return 受影响的行数
     */
    int delete(String tableName, String where);

    /**
     * 查询执行器
     *
     * @param sql sql
     * @return 更新数据执行器实例
     */
    QueryExecutor query(String sql);

    /**
     * 更新数据执行器
     *
     * @param tableName 表名
     * @param where     条件
     * @return 更新数据执行器实例
     */
    UpdateExecutor update(String tableName, String where);

    /**
     * 插入数据执行器
     *
     * @param tableName 表名
     * @return 插入数据执行器
     */
    InsertExecutor insert(String tableName);

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
