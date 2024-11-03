package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.MapExtends;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.utils.EntityUtil;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

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
     * @param entity standard java bean with annotation {@link javax.persistence.Entity @Entity}.
     * @return Args instance
     */
    public static <T> Args<Object> ofEntity(T entity) {
        return EntityUtil.entityToMap(entity, Args::new);
    }

    /**
     * Returns an entity.
     *
     * @param entityClass standard java bean class with annotation {@link javax.persistence.Entity @Entity}.
     * @param <E>         entity type
     * @return entity
     */
    @SuppressWarnings("unchecked")
    public <E> E toEntity(@NotNull Class<E> entityClass) {
        return EntityUtil.mapToEntity((Map<String, Object>) this, entityClass);
    }
}
