package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.dsl.types.FieldReference;

import javax.persistence.Column;
import java.beans.Introspector;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class EntityUtil {
    private static final Map<FieldReference<?>, String> columnCache = new ConcurrentHashMap<>();

    /**
     * Convert entity to map.
     *
     * @param entity     entity
     * @param mapBuilder map builder
     * @param <T>        map type
     * @param <E>        entity type
     * @return map
     * @see Column @Column
     */
    public static <T extends Map<String, Object>, E> T entityToMap(E entity, Function<Integer, T> mapBuilder) {
        return ObjectUtil.entityToMap(entity, EntityUtil::getColumnName, mapBuilder);
    }

    /**
     * Convert map to entity.
     *
     * @param source map
     * @param clazz  entity class
     * @param <T>    entity type
     * @return entity
     * @see Column @Column
     */
    public static <T> T mapToEntity(Map<String, Object> source, Class<T> clazz) {
        return ObjectUtil.mapToEntity(source, clazz, EntityUtil::getColumnName, null);
    }

    /**
     * Get the entity column name.
     *
     * @param field field
     * @return column name
     * @see Column @Column
     */
    public static String getColumnName(Field field) {
        String name = field.getName();
        if (field.isAnnotationPresent(Column.class)) {
            String column = field.getAnnotation(Column.class).name();
            if (!column.isEmpty()) {
                name = column;
            }
        }
        return name;
    }

    public static <T> String getFieldNameWithCache(FieldReference<T> fieldRef) {
        return columnCache.computeIfAbsent(fieldRef, EntityUtil::getFieldName);
    }

    public static <T> String getFieldName(FieldReference<T> fieldRef) {
        try {
            Method writeReplace = fieldRef.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            SerializedLambda serializedLambda = (SerializedLambda) writeReplace.invoke(fieldRef);

            String methodName = serializedLambda.getImplMethodName();
            if (methodName.startsWith("get") && methodName.length() > 3) {
                return Introspector.decapitalize(methodName.substring(3));
            }
            if (methodName.startsWith("is") && methodName.length() > 2) {
                return Introspector.decapitalize(methodName.substring(2));
            }
            throw new IllegalArgumentException("Invalid method reference: " + methodName);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to parse lambda expression", e);
        }
    }
}
