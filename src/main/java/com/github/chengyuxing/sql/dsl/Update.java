package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.dsl.clause.Where;

import java.util.function.Function;

/**
 * Dsl update operator.
 *
 * @param <T> entity type
 */
public abstract class Update<T> {
    protected boolean ignoreNull = false;

    /**
     * Ignore null value of the sets part.
     *
     * @param ignoreNull true ignore or false
     * @return Update
     */
    public Update<T> ignoreNull(boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
        return this;
    }

    /**
     * Ignore null value of the sets part.
     *
     * @return Update
     */
    public Update<T> ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    /**
     * Update by id.
     *
     * @return affected row
     * @see javax.persistence.Id @Id
     */
    public abstract int byId();

    /**
     * Update by ...
     *
     * @param where where builder
     * @return affected rows
     */
    public abstract int by(Function<Where<T>, Where<T>> where);
}
