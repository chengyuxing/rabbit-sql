package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.common.MethodReference;
import com.github.chengyuxing.sql.dsl.clause.Where;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * DSL Update operator.
 *
 * @param <T> entity type
 */
public interface Update<T> {
    /**
     * Enabling updates allows setting null values,
     * by default update statement set part will be excluded null values.
     * <p>Example: {@code {id: 1, name: 'cyx', address: null}}</p>
     * Enable:
     * <blockquote><pre>
     *     update user set name = :name, address = :address where id = :id
     * </pre></blockquote>
     * Disable (default):
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
    int save(@NotNull Iterable<T> entities);

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

