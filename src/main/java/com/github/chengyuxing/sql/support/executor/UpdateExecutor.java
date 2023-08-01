package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 更新执行器
 */
public abstract class UpdateExecutor {
    protected final String tableName;
    protected final String where;
    protected boolean safe = false;
    protected boolean fast = false;
    protected boolean ignoreNull = false;

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
     * 执行表字段查询并根据表字段筛选数据中不存在的字段
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
     * 忽略null值
     *
     * @return 插入构建器
     */
    public UpdateExecutor ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    /**
     * 更新保存<br>
     * e.g. {@code update(<table>, <List>, "id = :id")}<br>
     * 关于此方法的说明举例：
     * <blockquote>
     * <pre>
     *  参数： {id:14, name:'cyx', address:'kunming'}
     *  条件："id = :id"
     *  生成：update{@code <table>} set name = :name, address = :address
     *       where id = :id
     *  </pre>
     * 解释：where中至少指定一个传名参数，数据中必须包含where条件中的所有传名参数
     * </blockquote>
     *
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Map<String, ?> data);

    /**
     * 更新保存<br>
     * e.g. {@code update(<table>, <List>, "id = :id")}<br>
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
     * @param data 数据
     * @return 受影响的行数
     */
    public abstract int save(Collection<? extends Map<String, ?>> data);

    /**
     * 更新一个实体
     *
     * @param entity 标准java bean实体
     * @return 受影响的行数
     */
    public int saveEntity(Object entity) {
        return save(DataRow.fromEntity(entity));
    }

    /**
     * 更新一组实体
     *
     * @param entities 一组标准java bean实体类
     * @return 受影响的行数
     */
    public int saveEntities(Collection<?> entities) {
        return save(entities.stream().map(DataRow::fromEntity).collect(Collectors.toList()));
    }
}
