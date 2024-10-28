package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.dsl.type.ColumnReference;

import javax.persistence.Column;
import java.beans.Introspector;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class EntityUtil {
    private static final Map<ColumnReference<?>, String> columnCache = new ConcurrentHashMap<>();

    public static <T extends Map<String, Object>, E> T entity2map(E entity, Function<Integer, T> mapBuilder) {
        return ObjectUtil.entity2map(entity, f -> {
            String name = f.getName();
            if (f.isAnnotationPresent(Column.class)) {
                String column = f.getAnnotation(Column.class).name();
                if (!column.isEmpty()) {
                    name = column;
                }
            }
            return name;
        }, mapBuilder);
    }

    public static <T> T map2entity(Map<String, Object> source, Class<T> entityClass) {
        return ObjectUtil.map2entity(source, entityClass,
                f -> {
                    String name = f.getName();
                    if (f.isAnnotationPresent(Column.class)) {
                        String column = f.getAnnotation(Column.class).name();
                        if (!column.isEmpty()) {
                            name = column;
                        }
                    }
                    return name;
                }, null);
    }

    public static <T> String getFieldNameWithCache(ColumnReference<T> columnRef) {
        return columnCache.computeIfAbsent(columnRef, EntityUtil::getFieldName);
    }

    public static <T> String getFieldName(ColumnReference<T> columnRef) {
        try {
            Method writeReplace = columnRef.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            SerializedLambda serializedLambda = (SerializedLambda) writeReplace.invoke(columnRef);

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
