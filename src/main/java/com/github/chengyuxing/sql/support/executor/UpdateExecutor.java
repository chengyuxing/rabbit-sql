package com.github.chengyuxing.sql.support.executor;

import java.util.Collection;
import java.util.Map;

/**
 * 更新执行器
 */
public abstract class UpdateExecutor {
    protected final String tableName;
    protected final String where;
    protected boolean safe = false;
    protected boolean fast = false;

    /**
     * 构造函数和
     *
     * @param tableName 表名
     * @param where     更新必要的条件
     */
    public UpdateExecutor(String tableName, String where) {
        this.tableName = tableName;
        this.where = where;
    }

    /**
     * 启用安全更新<br>
     * 执行表字段查询并根据表字段筛选数据中存在的字段
     *
     * @return 更新构建器
     */
    public UpdateExecutor safe() {
        this.safe = true;
        return this;
    }

    /**
     * 启用快速更新（非预编译SQL）<br>
     * 注：不支持二进制对象
     *
     * @return 更新构建器
     */
    public UpdateExecutor fast() {
        this.fast = true;
        return this;
    }

    /**
     * 更新保存
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Map<String, ?> data);

    /**
     * 更新保存
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Collection<? extends Map<String, ?>> data);
}
