package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.dsl.Delete;
import com.github.chengyuxing.sql.dsl.Insert;
import com.github.chengyuxing.sql.dsl.Query;
import com.github.chengyuxing.sql.dsl.Update;

/**
 * Entity executor.
 *
 * @param <T> entity type
 */
public interface EntityExecutor<T> {
    /**
     * Query.
     *
     * @param <SELF> query builder
     * @return Query instance
     */
    <SELF extends Query<T, SELF>> Query<T, SELF> query();

    /**
     * Insert.
     *
     * @return Insert object
     */
    Insert<T> insert();

    /**
     * Update.
     *
     * @return Update object
     */
    Update<T> update();

    /**
     * Delete.
     *
     * @return Delete object
     */
    Delete<T> delete();
}
