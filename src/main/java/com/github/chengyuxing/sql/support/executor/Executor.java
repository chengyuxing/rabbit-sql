package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;

import java.util.*;

/**
 * 通用执行器
 */
public abstract class Executor {
    protected final String sql;
    protected final String[] more;
    protected Map<String, Object> args = new HashMap<>();

    /**
     * 执行器
     *
     * @param sql  一条sql
     * @param more 更多sql
     */
    protected Executor(String sql, String... more) {
        this.sql = sql;
        this.more = more;
    }

    /**
     * 设置sql中的参数字典
     *
     * @param args 参数字典
     * @return 查询执行器
     */
    public Executor args(Map<String, Object> args) {
        if (args == null) {
            this.args = new HashMap<>();
        } else {
            this.args = args;
        }
        return this;
    }

    /**
     * 添加sql中的参数
     *
     * @param key   键
     * @param value 值
     * @return 查询执行器
     */
    public Executor arg(String key, Object value) {
        this.args.put(key, value);
        return this;
    }

    /**
     * 批量执行非查询sql（非预编译sql，不支持参数）
     *
     * @return 每条sql执行的结果
     * @see com.github.chengyuxing.sql.support.JdbcSupport#executeBatch(List) executeBatch
     */
    public abstract int[] executeBatch();

    /**
     * 执行一条原始sql
     *
     * @return 根据不同的类型返回不同的结果
     * @see com.github.chengyuxing.sql.support.JdbcSupport#execute(String, Map) execute
     */
    public abstract DataRow execute();
}
