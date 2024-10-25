package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.MapExtends;
import com.github.chengyuxing.common.utils.ObjectUtil;

import java.util.HashMap;

/**
 * Simple sql args tool.
 *
 * @param <V> value type
 */
public final class Args<V> extends HashMap<String, V> implements MapExtends<Args<V>, V> {
    /**
     * Constructs a new empty Args.
     */
    public Args() {
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
        return ObjectUtil.pairs2map(i -> Args.of(), input);
    }

    /**
     * Returns an Args from standard java bean entity.
     *
     * @param entity standard java bean.
     * @return Args instance
     */
    public static Args<Object> ofEntity(Object entity) {
        return ObjectUtil.entity2map(entity, i -> Args.of());
    }
}
