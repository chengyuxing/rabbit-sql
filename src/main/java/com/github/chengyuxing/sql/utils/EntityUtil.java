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

    public static <T extends Map<String, Object>, E> T entityToMap(E entity, Function<Integer, T> mapBuilder) {
        return ObjectUtil.entityToMap(entity, EntityUtil::getColumnName, mapBuilder);
    }

    public static <T> T mapToEntity(Map<String, Object> source, Class<T> entityClass) {
        return ObjectUtil.mapToEntity(source, entityClass, EntityUtil::getColumnName, null);
    }

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
