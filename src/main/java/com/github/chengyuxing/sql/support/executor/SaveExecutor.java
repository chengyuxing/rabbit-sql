package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Save executor.
 *
 * @param <T> entity type
 */
public abstract class SaveExecutor<T> {
    protected boolean safe = false;
    protected boolean ignoreNull = false;

    /**
     * Enable safe mode.<br>
     * Ignore data key if table all fields not contains key.
     *
     * @return SaveExecutor
     */
    public SaveExecutor<T> safe() {
        this.safe = true;
        return this;
    }

    /**
     * Enable safe mode.<br>
     * Ignore data key if table all fields not contains key.
     *
     * @param enableSafe enable safe mode or not
     * @return SaveExecutor
     */
    public SaveExecutor<T> safe(boolean enableSafe) {
        this.safe = enableSafe;
        return this;
    }

    /**
     * Enable ignore null.
     *
     * @return SaveExecutor
     */
    public SaveExecutor<T> ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    /**
     * Enable ignore null.
     *
     * @param enableIgnoreNull enable ignore null or not
     * @return SaveExecutor
     */
    public SaveExecutor<T> ignoreNull(boolean enableIgnoreNull) {
        this.ignoreNull = enableIgnoreNull;
        return this;
    }

    /**
     * Execute save.
     *
     * @param data data
     * @return affected row count
     */
    public abstract int save(Map<String, ?> data);

    /**
     * Execute save.
     *
     * @param data data
     * @return affected row count
     */
    public abstract int save(Collection<? extends Map<String, ?>> data);

    /**
     * Execute save.
     *
     * @param entity standard java bean entity
     * @return affected row count
     */
    public int saveEntity(T entity) {
        return save(DataRow.ofEntity(entity));
    }

    /**
     * Execute save.
     *
     * @param entities standard java bean entity
     * @return affected row count
     */
    public int saveEntities(Collection<T> entities) {
        return save(entities.stream().map(DataRow::ofEntity).collect(Collectors.toList()));
    }
}
