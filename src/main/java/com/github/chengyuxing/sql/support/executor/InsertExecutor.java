package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 插入执行器
 */
public abstract class InsertExecutor {
    protected final String tableName;
    protected boolean safe = false;
    protected boolean fast = false;

    /**
     * 插入执行器实例
     *
     * @param tableName 表名
     */
    public InsertExecutor(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 启用安全插入<br>
     * 执行表字段查询并根据表字段筛选数据中不存在的字段
     *
     * @return 插入构建器
     */
    public InsertExecutor safe() {
        this.safe = true;
        return this;
    }

    /**
     * 启用快速插入（非预编译SQL）<br>
     * 注：不支持二进制对象
     *
     * @return 插入构建器
     */
    public InsertExecutor fast() {
        this.fast = true;
        return this;
    }

    /**
     * 插入保存
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Map<String, ?> data);

    /**
     * 插入保存一个实体
     *
     * @param entity 标准java bean实体
     * @return 受影响的行数
     */
    public int saveEntity(Object entity) {
        return save(DataRow.fromEntity(entity));
    }

    /**
     * 插入保存
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Collection<? extends Map<String, ?>> data);

    /**
     * 插入保存一组实体
     *
     * @param entities 一组标准java bean实体类
     * @return 受影响的行数
     */
    public int saveEntities(Collection<Object> entities) {
        return save(entities.stream().map(DataRow::fromEntity).collect(Collectors.toList()));
    }
}
