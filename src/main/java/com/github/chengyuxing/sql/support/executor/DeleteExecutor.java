package com.github.chengyuxing.sql.support.executor;

import java.util.HashMap;
import java.util.Map;

/**
 * 删除执行器
 */
public abstract class DeleteExecutor {
    protected final String tableName;
    protected Map<String, Object> args = new HashMap<>();

    /**
     * 删除执行器
     *
     * @param tableName 表名
     */
    public DeleteExecutor(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 添加sql中的参数
     *
     * @param key   键
     * @param value 值
     * @return 查询执行器
     */
    public DeleteExecutor arg(String key, Object value) {
        this.args.put(key, value);
        return this;
    }

    /**
     * 设置sql中的参数字典
     *
     * @param args 参数字典
     * @return 查询执行器
     */
    public DeleteExecutor args(Map<String, Object> args) {
        if (args == null) {
            this.args = new HashMap<>();
        } else {
            this.args = args;
        }
        return this;
    }

    /**
     * 执行删除操作
     *
     * @param where 必要的where条件，e.g: {@code id = :id}
     * @return 受影响的行数
     */
    public abstract int execute(String where);
}
