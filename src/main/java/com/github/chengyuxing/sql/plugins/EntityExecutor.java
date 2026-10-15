package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.dsl.Delete;
import com.github.chengyuxing.sql.dsl.Insert;
import com.github.chengyuxing.sql.dsl.Query;
import com.github.chengyuxing.sql.dsl.Update;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * Entity executor.
 *
 * @param <T> entity type
 */
public interface EntityExecutor<T> {
    /**
     * Query.
     *
     * @param queryId the {@link com.github.chengyuxing.sql.Baki#IDENTIFIER identifier} into args for the query,
     *                not a primary-key filter
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
     * Find an entity using the single primary key defined by the entity metadata provider.
     *
     * @param id non-null primary-key value
     * @return matching entity, or an empty optional if no row exists
     * @throws IllegalArgumentException if {@code id} is null
     */
    @NotNull Optional<T> findById(@NotNull Object id);

    /**
     * Check for a row by its single primary key without loading an entity.
     *
     * @param id non-null primary-key value
     * @return whether a matching row exists
     * @throws IllegalArgumentException if id is null
     */
    boolean existsById(@NotNull Object id);

    /**
     * Delete a row by its single primary key.
     *
     * @param id non-null primary-key value
     * @return affected row count, zero if no row matches
     * @throws IllegalArgumentException if id is null
     */
    int deleteById(@NotNull Object id);

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
