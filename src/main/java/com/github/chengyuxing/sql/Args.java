package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.MapExtends;
import com.github.chengyuxing.common.utils.ObjectUtil;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.function.Function;

/**
 * Simple args tool.
 *
 * @param <V> value type
 */
public final class Args<V> extends LinkedHashMap<String, V> implements MapExtends<Args<V>, V> {
    /**
     * Constructs a new empty Args.
     */
    public Args() {
    }

    public Args(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Returns an empty Args.
     *
     * @param <V> value type
     * @return empty Args instance
     */
    public static <V> Args<V> of() {
        return new Args<>();
    }

    /**
     * Returns an Args with generic initial value.
     *
     * @param k   key
     * @param v   value
     * @param <V> value type
     * @return generic Args instance
     */
    public static <V> Args<V> of(String k, V v) {
        Args<V> args = of();
        args.put(k, v);
        return args;
    }

    /**
     * Returns an Args from more than one pairs of value.
     *
     * @param input key-value pairs: k v, k v, ...
     * @return Args instance
     */
    public static Args<Object> of(Object... input) {
        return ObjectUtil.pairsToMap(i -> Args.of(), input);
    }

    /**
     * Returns an Args from standard java bean entity.
     *
     * @param entity standard java bean
     * @param <T>    entity type
     * @return Args instance
     */
    public static <T> Args<Object> ofEntity(T entity) {
        return ObjectUtil.entityToMap(entity, Args::new);
    }

    /**
     * Returns an Args from standard java bean entity.
     *
     * @param entity      standard java bean (entity field) -&gt; (map property)
     * @param fieldMapper entity field mapper
     * @param <T>         entity type
     * @return Args instance
     */
    public static <T> Args<Object> ofEntity(T entity, @NotNull Function<Field, String> fieldMapper) {
        return ObjectUtil.entityToMap(entity, fieldMapper, Args::new);
    }
}
