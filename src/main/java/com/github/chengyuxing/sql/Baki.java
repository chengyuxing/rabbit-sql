package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.support.executor.DeleteExecutor;
import com.github.chengyuxing.sql.support.executor.InsertExecutor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.UpdateExecutor;
import com.github.chengyuxing.sql.types.Param;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * 数据库基本操作通用接口
 */
public interface Baki {
    /**
     * 查询执行器
     *
     * @param sql sql或sql名
     * @return 查询执行器
     */
    QueryExecutor query(String sql);

    /**
     * 更新执行器
     *
     * @param tableName 表名
     * @param where     条件
     * @return 更新执行器
     */
    UpdateExecutor update(String tableName, String where);

    /**
     * 插入执行器
     *
     * @param tableName 表名
     * @return 插入执行器
     */
    InsertExecutor insert(String tableName);

    /**
     * 删除执行器
     *
     * @param tableName 表名
     * @return 删除执行器
     */
    DeleteExecutor delete(String tableName);

    /**
     * 执行一条sql（ddl、dml、query、plsql）
     *
     * @param sql  sql或sql名
     * @param args 参数
     * @return 执行结果
     */
    DataRow execute(String sql, Map<String, ?> args);

    /**
     * 执行一条sql（ddl、dml、query、plsql）
     *
     * @param sql sql或sql名
     * @return 执行结果
     */
    DataRow execute(String sql);

    /**
     * 批量执行非查询sql（非预编译）
     *
     * @param sqls sql或sql名
     * @return 每条sql的执行结果
     */
    int[] executeBatch(String... sqls);

    /**
     * 批量执行非查询sql（非预编译）
     *
     * @param namedSql 命名参数的sql
     * @param data     数据
     * @return 每条sql的执行结果
     */
    int[] executeBatch(String namedSql, Collection<? extends Map<String, ?>> data);

    /**
     * 执行存储过程或函数
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return 一个或多个结果或空对象
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
