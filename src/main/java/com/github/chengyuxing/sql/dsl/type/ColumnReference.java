package com.github.chengyuxing.sql.dsl.type;

import java.io.Serializable;

/**
 * Entity column getter method reference.
 *
 * @param <T> entity type
 */
@FunctionalInterface
public interface ColumnReference<T> extends Serializable {
    Object apply(T t);
}
