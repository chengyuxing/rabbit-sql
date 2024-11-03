package com.github.chengyuxing.sql.dsl.types;

import java.io.Serializable;

/**
 * Entity column getter method reference.
 *
 * @param <T> entity type
 */
@FunctionalInterface
public interface FieldReference<T> extends Serializable {
    Object apply(T t);
}
