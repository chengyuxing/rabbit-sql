package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.support.executor.DeleteExecutor;
import com.github.chengyuxing.sql.support.executor.Executor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.SaveExecutor;
import com.github.chengyuxing.sql.types.Param;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
     * 更新执行器<br>
     * 关于此方法的说明举例：
     * <blockquote>
     * <pre>
     *  参数： {id:14, name:'cyx', address:'kunming'},{...}...
     *  条件："id = :id"
     *  生成：update{@code <table>} set name = :name, address = :address
     *       where id = :id
     *  </pre>
     * 解释：where中至少指定一个传名参数，数据中必须包含where条件中的所有传名参数
     * </blockquote>
     *
     * @param tableName 数据
     * @param where     条件
     * @return 受影响的行数
     */
    SaveExecutor update(String tableName, String where);

    /**
     * 插入执行器
     *
     * @param tableName 表名
     * @return 插入执行器
     */
    SaveExecutor insert(String tableName);

    /**
     * 删除执行器
     *
     * @param tableName 表名
     * @return 删除执行器
     */
    DeleteExecutor delete(String tableName);

    /**
     * 通用执行器
     *
     * @param sql sql
     * @return 通用执行器
     */
    Executor of(String sql);

    /**
     * 执行存储过程或函数
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return 一个或多个结果或空对象
     * @see Args Args&lt;Param&gt;
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
