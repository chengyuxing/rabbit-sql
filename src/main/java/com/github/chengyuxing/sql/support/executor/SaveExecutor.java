package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 通用保存接口
 */
public abstract class SaveExecutor {
    protected boolean safe = false;
    protected boolean fast = false;
    protected boolean ignoreNull = false;

    /**
     * 启用安全处理<br>
     * 执行表字段查询并根据表字段筛选数据中不存在的字段
     *
     * @return 插入构建器
     */
    public SaveExecutor safe() {
        this.safe = true;
        return this;
    }

    /**
     * 是否启用安全插入<br>
     * 执行表字段查询并根据表字段筛选数据中不存在的字段
     *
     * @param enableSafe 是否启用安全处理
     * @return 插入构建器
     */
    public SaveExecutor safe(boolean enableSafe) {
        this.safe = enableSafe;
        return this;
    }

    /**
     * 启用快速保存（非预编译SQL）<br>
     * 注：不支持二进制对象
     *
     * @return 插入构建器
     */
    public SaveExecutor fast() {
        this.fast = true;
        return this;
    }

    /**
     * 是否启用快速保存（非预编译SQL）<br>
     * 注：不支持二进制对象
     *
     * @param enableFast 是否启用快速保存
     * @return 插入构建器
     */
    public SaveExecutor fast(boolean enableFast) {
        this.fast = enableFast;
        return this;
    }

    /**
     * 忽略null值
     *
     * @return 插入构建器
     */
    public SaveExecutor ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    /**
     * 是否忽略null值
     *
     * @param enableIgnoreNull 是否启用忽略null值
     * @return 插入构建器
     */
    public SaveExecutor ignoreNull(boolean enableIgnoreNull) {
        this.ignoreNull = enableIgnoreNull;
        return this;
    }

    /**
     * 保存
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Map<String, ?> data);

    /**
     * 保存一个实体
     *
     * @param entity 标准java bean实体
     * @return 受影响的行数
     */
    public int saveEntity(Object entity) {
        return save(DataRow.fromEntity(entity));
    }

    /**
     * 保存
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Collection<? extends Map<String, ?>> data);

    /**
     * 保存一组实体
     *
     * @param entities 一组标准java bean实体类
     * @return 受影响的行数
     */
    public int saveEntities(Collection<?> entities) {
        return save(entities.stream().map(DataRow::fromEntity).collect(Collectors.toList()));
    }
}
