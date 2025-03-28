package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.sql.dsl.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

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
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.0")
    <SELF extends Query<T, SELF>> Query<T, SELF> query();

    /**
     * Insert.
     *
     * @param entity entity
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.0")
    int insert(@NotNull T entity);

    /**
     * Batch insert.
     *
     * @param entities entities
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.0")
    int insert(@NotNull Collection<T> entities);

    /**
     * Update by {@link jakarta.persistence.Id id}.
     *
     * @param entity     entity
     * @param ignoreNull ignore null value or not for update sets
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    int update(@NotNull T entity, boolean ignoreNull);

    /**
     * Batch update by {@link jakarta.persistence.Id id}.
     * <p>Notice: the real update statement depends on first data,
     * it means 'ignoreNull' just available on first data.</p>
     *
     * @param entities   entities
     * @param ignoreNull ignore null value or not for update sets
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    int update(@NotNull Collection<T> entities, boolean ignoreNull);

    /**
     * Delete by {@link jakarta.persistence.Id id}.
     *
     * @param entity entity
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    int delete(@NotNull T entity);

    /**
     * Batch delete by {@link jakarta.persistence.Id id}.
     *
     * @param entities entities
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    int delete(@NotNull Collection<T> entities);
}
