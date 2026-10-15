package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.common.MethodReference;
import com.github.chengyuxing.sql.dsl.clause.Where;
import com.github.chengyuxing.sql.types.BatchResult;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * DSL Update operator.
 *
 * @param <T> entity type
 */
public interface Update<T> {
    /**
     * Update only the specified mapped properties in primary-key saves.
     * Repeated calls accumulate properties for this Update instance, including
     * batch saves. Null handling follows {@link #withNullValues()}.
     * Cannot be combined with {@link #exclude(MethodReference[])} or conditional
     * {@link #where(Function)} updates.
     *
     * @param fields one or more entity getter references, not database column names
     * @return Update object
     * @throws IllegalArgumentException if no fields are supplied, or a property is
     * unmapped, a primary key, or not updatable
     * @throws IllegalStateException if exclusions have already been configured
     */
    @SuppressWarnings("unchecked")
    Update<T> only(@NotNull MethodReference<T>... fields);

    /**
     * Exclude mapped entity properties from the SET clause of primary-key saves.
     * Repeated calls accumulate exclusions for this Update instance; batch saves
     * apply the same exclusions to every entity. Primary-key lookup and metadata
     * updatable restrictions are unchanged. Null handling follows {@link #withNullValues()}.
     * If no columns remain, save returns zero (one zero per entity for a batch).
     * Cannot be combined with conditional {@link #where(Function)} updates.
     * Cannot be combined with {@link #only(MethodReference[])}; an empty field list
     * otherwise leaves the configuration unchanged.
     *
     * @param fields entity getter references, not database column names
     * @return Update object
     * @throws IllegalArgumentException if a reference does not resolve to a mapped property
     * @throws IllegalStateException if only-properties have already been configured
     */
    @SuppressWarnings("unchecked")
    Update<T> exclude(@NotNull MethodReference<T>... fields);

    /**
     * Enabling updates allows setting null values
     * and invoke JDBC {@link java.sql.Statement#executeBatch() executeBatchUpdate} otherwise update each item with for loop.
     * <p>
     * By default, update statement set part will be excluded null values.
     * <p>Example: {@code {id: 1, name: 'cyx', address: null}}</p>
     * Enable (do execute batch update):
     * <blockquote><pre>
     *     update user set name = :name, address = :address where id = :id
     * </pre></blockquote>
     * Disable (default, do foreach):
     * <blockquote><pre>
     *     update user set name = :name where id = :id
     * </pre></blockquote>
     *
     * @return Update object
     */
    Update<T> withNullValues();

    /**
     * Update by primary key.
     *
     * @param entity entity
     * @return affected row count
     * @see #withNullValues()
     */
    int save(@NotNull T entity);

    /**
     * Multiple update by primary key.
     *
     * @param entities entities
     * @return affected row count
     * @see #withNullValues()
     */
    BatchResult save(@NotNull Iterable<T> entities);

    /**
     * Update the specified columns by condition.
     *
     * @param where where condition
     * @return UpdateSetter
     */
    UpdateSetter<T> where(Function<Where<T>, Where<T>> where);

    /**
     * Update's setter builder.
     *
     * @param <T> entity type
     */
    interface UpdateSetter<T> {
        /**
         * Add value ready to update.
         *
         * @param column column name
         * @param value  value
         * @return UpdateSetter
         */
        UpdateSetter<T> set(@NotNull MethodReference<T> column, Object value);

        /**
         * Do update save.
         *
         * @return affected row count
         */
        int save();
    }
}

