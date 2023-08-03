package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;

import java.sql.PreparedStatement;
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
     * 启用安全模式<br>
     * 执行表字段查询并根据表字段筛选数据中不存在的字段
     *
     * @return 保存构建器
     */
    public SaveExecutor safe() {
        this.safe = true;
        return this;
    }

    /**
     * 是否启用安全模式<br>
     * 执行表字段查询并根据表字段筛选数据中不存在的字段
     *
     * @param enableSafe 是否启用安全模式
     * @return 保存构建器
     */
    public SaveExecutor safe(boolean enableSafe) {
        this.safe = enableSafe;
        return this;
    }

    /**
     * 启用快速模式<br>
     * 如果执行保存多条数据，则根据第一条数据来生成满足条件的具体sql，例如:
     * <blockquote>
     * <pre>参数：[
     *      {name:'abc', age:30},
     *      {name:'123', 'age':30, address:'kunming'},
     *      ...
     *    ]</pre>
     * <pre>结果：insert into ... (name, age) values (:name, :age)</pre>
     * 如上第二条 <code>address</code> 字段将被忽略。
     * </blockquote>
     * 同样的 {@link #ignoreNull()} 只对用来生成sql的第一条数据产生效果。
     *
     * @return 保存构建器
     * @see PreparedStatement#executeBatch()
     */
    public SaveExecutor fast() {
        this.fast = true;
        return this;
    }

    /**
     * 是否启用快速模式<br>
     *
     * @param enableFast 是否启用快速模式
     * @return 保存构建器
     * @see #fast()
     */
    public SaveExecutor fast(boolean enableFast) {
        this.fast = enableFast;
        return this;
    }

    /**
     * 忽略null值
     *
     * @return 保存构建器
     */
    public SaveExecutor ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    /**
     * 是否忽略null值
     *
     * @param enableIgnoreNull 是否启用忽略null值
     * @return 保存构建器
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
     * 保存
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Collection<? extends Map<String, ?>> data);

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
     * 保存一组实体
     *
     * @param entities 一组标准java bean实体类
     * @return 受影响的行数
     */
    public int saveEntities(Collection<?> entities) {
        return save(entities.stream().map(DataRow::fromEntity).collect(Collectors.toList()));
    }
}
