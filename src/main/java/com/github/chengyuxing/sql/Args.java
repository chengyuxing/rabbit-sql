package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.MapExtends;
import com.github.chengyuxing.common.utils.Jackson;
import com.github.chengyuxing.common.utils.ReflectUtil;

import java.beans.IntrospectionException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;

/**
 * 简单参数工具类
 *
 * @param <V> 值类型参数
 */
public final class Args<V> extends HashMap<String, V> implements MapExtends<V> {
    public Args() {
    }

    /**
     * 创建一个空的参数对象
     *
     * @param <V> 值类型参数
     * @return 空的参数对象
     */
    public static <V> Args<V> of() {
        return new Args<>();
    }

    /**
     * 创建一个具有初始值类型的参数对象
     *
     * @param k   键
     * @param v   值
     * @param <V> 值类型参数
     * @return 限定值类型参数对象
     */
    public static <V> Args<V> of(String k, V v) {
        Args<V> args = of();
        args.put(k, v);
        return args;
    }

    /**
     * 从一组键值对创建一个参数对象
     *
     * @param input 键值对 k v，k v...
     * @return 参数对象
     */
    public static Args<Object> of(Object... input) {
        if ((input.length & 1) != 0) {
            throw new IllegalArgumentException("key value are not a pair.");
        }
        int capacity = input.length >> 1;
        Args<Object> args = of();
        for (int i = 0; i < capacity; i++) {
            int idx = i << 1;
            args.put(input[idx].toString(), input[idx + 1]);
        }
        return args;
    }

    /**
     * 从一个标准的javaBean实体转为参数对象
     *
     * @param entity 实体
     * @return 参数对象
     */
    public static Args<Object> ofEntity(Object entity) {
        if (Objects.isNull(entity)) return of();
        try {
            Args<Object> args = of();
            Class<?> clazz = entity.getClass();
            for (Method method : ReflectUtil.getRWMethods(entity.getClass()).getItem1()) {
                Field classField = ReflectUtil.getGetterField(clazz, method);
                if (Objects.isNull(classField)) {
                    continue;
                }
                Object value = method.invoke(entity);
                args.put(classField.getName(), value);
            }
            return args;
        } catch (IllegalAccessException | IntrospectionException | InvocationTargetException | NoSuchFieldException e) {
            throw new RuntimeException("convert to DataRow error: ", e);
        }
    }

    /**
     * 从一个json对象字符串创建一个参数对象
     *
     * @param json json对象字符串 e.g. {@code {"a":1,"b":2}}
     * @return 参数对象
     */
    public static Args<Object> ofJson(String json) {
        if (Objects.isNull(json)) return of();
        //noinspection unchecked
        return Jackson.toObject(json, Args.class);
    }

    /**
     * 从map转换到参数对象
     *
     * @param other map
     * @return 参数对象
     */
    public static Args<Object> ofMap(Map<?, ?> other) {
        if (Objects.isNull(other)) return of();
        Args<Object> args = of();
        for (Map.Entry<?, ?> e : other.entrySet()) {
            args.put(e.getKey().toString(), e.getValue());
        }
        return args;
    }

    /**
     * 添加一个键值对
     *
     * @param k 键
     * @param v 值
     * @return 参数对象
     */
    public Args<V> add(String k, V v) {
        put(k, v);
        return this;
    }

    /**
     * 更新一个值
     *
     * @param key     键
     * @param updater 值更新器（{@code 旧值 -> 新值}）
     * @return 是否更新成功
     */
    @SuppressWarnings("unchecked")
    public <T> boolean updateValue(String key, Function<T, V> updater) {
        if (containsKey(key)) {
            put(key, updater.apply((T) get(key)));
            return true;
        }
        return false;
    }

    /**
     * 更新一个键名
     *
     * @param oldKey 旧键名
     * @param newKey 新键名
     * @return 是否更新成功
     */
    public boolean updateKey(String oldKey, String newKey) {
        if (containsKey(oldKey)) {
            put(newKey, remove(oldKey));
            return true;
        }
        return false;
    }

    /**
     * 更新全部键名
     *
     * @param updater 键名更新器（{@code 旧键名 -> 新键名}）
     */
    public void updateKeys(Function<String, String> updater) {
        Object[] keys = keySet().toArray();
        for (Object key : keys) {
            if (key == null) {
                continue;
            }
            String newKey = updater.apply(key.toString());
            put(newKey, remove(key));
        }
        Arrays.fill(keys, null);
    }
}
