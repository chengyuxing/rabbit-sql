package com.github.chengyuxing.sql.plugins;

/**
 * Entity value mapper.
 */
@FunctionalInterface
public interface EntityValueMapper {
    /**
     * Convert the origin map value to entity filed value.
     *
     * @param valueType       value type
     * @param entityFieldType entity field type
     * @param value           value
     * @return the suitable value for the entity field
     */
    Object apply(Class<?> valueType, Class<?> entityFieldType, Object value);
}
