package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.dsl.clause.Where;

import java.util.function.Function;

/**
 * Dsl delete operator.
 *
 * @param <T> entity type
 */
public interface Delete<T> {
    /**
     * Delete by id.
     *
     * @return affected row
     * @see javax.persistence.Id @Id
     */
    int byId();

    /**
     * Delete by ...
     *
     * @param where where builder
     * @return affected row3
     */
    int by(Function<Where<T>, Where<T>> where);
}
