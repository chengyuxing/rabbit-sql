package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.types.Param;

import java.util.Collection;
import java.util.Map;

/**
 * 通用执行器
 */
public interface Executor {
    /**
     * 执行一条sql（ddl、dml、query、plsql）
     *
     * @return 查询结果
     * @see com.github.chengyuxing.sql.support.JdbcSupport#execute(String, Map) execute(String, Map)
     */
    DataRow execute();

    /**
     * 执行一条sql（ddl、dml、query、plsql）
     *
     * @param args 参数
     * @return 查询结果
     * @see com.github.chengyuxing.sql.support.JdbcSupport#execute(String, Map) execute(String, Map)
     */
    DataRow execute(Map<String, ?> args);

    /**
     * 批量执行非查询sql
     *
     * @param moreSql 更多的sql
     * @return 受影响的行数
     */
    int executeBatch(String... moreSql);

    /**
     * 批量执行dml语句
     *
     * @param data 数据
     * @return 受影响的行数
     */
    int executeBatch(Collection<? extends Map<String, ?>> data);

    /**
     * 执行存储过程或函数
     *
     * @param params 参数：{@link com.github.chengyuxing.sql.types.OUTParamType Args&lt;Param&gt;}
     * @return DataRow
     * @see com.github.chengyuxing.sql.support.JdbcSupport#executeCallStatement(String, Map) executeCallStatement(String, Map)
     */
    DataRow call(Map<String, Param> params);
}
