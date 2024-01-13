package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.MapExtends;
import com.github.chengyuxing.common.utils.ObjectUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;

/**
 * Simple sql args tool.
 *
 * @param <V> value type
 */
public final class Args<V> extends HashMap<String, V> implements MapExtends<V> {
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

    /**
     * Add a key-value.
     *
     * @param k key
     * @param v value
     * @return Args instance
     */
    public Args<V> add(String k, V v) {
        put(k, v);
        return this;
    }

    /**
     * Update a key name to another.
     *
     * @param oldKey old key name
     * @param newKey new key name
     */
    public void updateKey(String oldKey, String newKey) {
        if (containsKey(oldKey)) {
            put(newKey, remove(oldKey));
        }
    }

    /**
     * Update all key name.
     *
     * @param updater (old key name) {@code ->} (new key name)
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
