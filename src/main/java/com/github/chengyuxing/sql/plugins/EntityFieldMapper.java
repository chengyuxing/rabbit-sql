package com.github.chengyuxing.sql.plugins;

import java.lang.reflect.Field;

/**
 * Entity field mapper.
 */
@FunctionalInterface
public interface EntityFieldMapper {
    /**
     * Make entity field mapping to the target property
     *
     * @param field entity field
     * @return target property name
     */
    String apply(Field field);
}
