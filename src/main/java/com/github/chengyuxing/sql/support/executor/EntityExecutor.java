package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.sql.dsl.Query;
import com.github.chengyuxing.sql.dsl.clause.Where;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.Function;

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
     * Update by condition.
     *
     * @param entity     entity
     * @param ignoreNull ignore null value or not for update sets
     * @param where      where condition
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    int update(@NotNull T entity, boolean ignoreNull, Function<Where<T>, Where<T>> where);

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
     * Batch update by condition.
     * <p>Notice: the real update statement depends on first data,
     * it means 'ignoreNull' just available on first data.</p>
     *
     * @param entities   entities
     * @param ignoreNull ignore null value or not for update sets
     * @param where      where condition
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    int update(@NotNull Collection<T> entities, boolean ignoreNull, Function<Where<T>, Where<T>> where);

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
     * Delete by condition.
     *
     * @param entity entity
     * @param where  where condition
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    int delete(@NotNull T entity, Function<Where<T>, Where<T>> where);

    /**
     * Batch delete by {@link jakarta.persistence.Id id}.
     *
     * @param entities entities
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    int delete(@NotNull Collection<T> entities);

    /**
     * Batch delete by condition.
     *
     * @param entities entities
     * @param where    where condition
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    int delete(@NotNull Collection<T> entities, Function<Where<T>, Where<T>> where);
}
