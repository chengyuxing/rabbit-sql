package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.dsl.Delete;
import com.github.chengyuxing.sql.dsl.Insert;
import com.github.chengyuxing.sql.dsl.Query;
import com.github.chengyuxing.sql.dsl.Update;
import org.jetbrains.annotations.Nullable;

/**
 * Entity executor.
 *
 * @param <T> entity type
 */
public interface EntityExecutor<T> {
    /**
     * Query.
     *
     * @param queryId the {@link com.github.chengyuxing.sql.Baki#IDENTIFIER identifier} into args for the query
     * @param <SELF>  query builder
     * @return Query instance
     */
    <SELF extends Query<T, SELF>> Query<T, SELF> query(@Nullable Object queryId);

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
