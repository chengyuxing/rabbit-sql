package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.dsl.clause.Where;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * DSL delete operator.
 *
 * @param <T> entity type
 */
public interface Delete<T> {
    /**
     * Delete by primary key.
     *
     * @param entity entity
     * @return affected row count
     */
    int execute(@NotNull T entity);

    /**
     * Batch delete by primary key.
     *
     * @param entities entities
     * @return affected row count
     */
    int execute(@NotNull Iterable<T> entities);

    /**
     * Delete by condition.
     *
     * @param where where condition
     * @return Conditional
     */
    Conditional where(Function<Where<T>, Where<T>> where);

    /**
     * Delete's condition builder
     */
    interface Conditional {

        /**
         * Delete by condition.
         *
         * @return affected row count
         */
        int execute();
    }
}
