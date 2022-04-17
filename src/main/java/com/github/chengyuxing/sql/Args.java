package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.types.Param;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * sql参数构建工具类
 *
 * @param <V> 类型参数
 */
public class Args<V> extends HashMap<String, V> {
    public Args() {
    }

    public Args(Map<? extends String, ? extends V> m) {
        super(m);
    }

    /**
     * 创建一个空的参数集合
     *
     * @param <V> 类型参数
     * @return 空参数集合
     */
    public static <V> Args<V> create() {
        return new Args<>();
    }

    /**
     * 从一组键值对创建一个Args
     *
     * @param pairOfArgs 键值对 k v，k v...
     * @return Args对象
     */
    public static Args<Object> create(Object... pairOfArgs) {
        if (pairOfArgs.length == 0 || (pairOfArgs.length & 1) != 0) {
            throw new IllegalArgumentException("key value are not a pair.");
        }
        int length = pairOfArgs.length >> 1;
        Args<Object> args = create();
        for (int i = 0; i < length; i++) {
            args.put(pairOfArgs[i << 1].toString(), pairOfArgs[(i << 1) + 1]);
        }
        return args;
    }

    /**
     * 从一个Map创建一个Args对象
     *
     * @param m   map
     * @param <V> 类型参数
     * @return Args
     */
    public static <V> Args<V> of(Map<? extends String, ? extends V> m) {
        return new Args<>(m);
    }

    /**
     * 创建一个有初始值的参数集合
     *
     * @param key   名称
     * @param value 值
     * @param <V>   值类型参数
     * @return 有初始值的参数集合
     */
    public static <V> Args<V> of(String key, V value) {
        Args<V> args = new Args<>();
        args.add(key, value);
        return args;
    }

    /**
     * 创建一个空的存储过程参数类型集合
     *
     * @param key   名称
     * @param param 参数对象
     * @return 有初始值的存储过程参数集合
     */
    public static Args<Param> ofProcedure(String key, Param param) {
        Args<Param> args = new Args<>();
        args.add(key, param);
        return args;
    }

    /**
     * 链式添加键值
     *
     * @param key   名称
     * @param value 值
     * @return 参数集合
     */
    public Args<V> add(String key, V value) {
        put(key, value);
        return this;
    }

    /**
     * 移除值为null的所有元素
     *
     * @return 不存在null值的当前对象
     */
    public Args<V> removeIfAbsent() {
        Iterator<Entry<String, V>> iterator = entrySet().iterator();
        //为了内部一点性能就不使用函数接口了
        //noinspection Java8CollectionRemoveIf
        while (iterator.hasNext()) {
            if (iterator.next().getValue() == null) {
                iterator.remove();
            }
        }
        return this;
    }

    /**
     * 移除值为null且不包含在指定keys中的所有元素
     *
     * @param keys 需忽略的键名集合
     * @return 移除匹配元素后的当前对象
     */
    public Args<V> removeIfAbsentExclude(String... keys) {
        Iterator<Entry<String, V>> iterator = entrySet().iterator();
        //为了内部一点性能就不使用函数接口了
        //noinspection Java8CollectionRemoveIf
        while (iterator.hasNext()) {
            Entry<String, V> e = iterator.next();
            if (e.getValue() == null && !Arrays.asList(keys).contains(e.getKey())) {
                iterator.remove();
            }
        }
        return this;
    }

    /**
     * 根据条件移除元素
     *
     * @param predicate 条件
     * @return 移除匹配元素后的当前对象
     */
    public Args<V> removeIf(BiPredicate<String, V> predicate) {
        Iterator<Map.Entry<String, V>> iterator = entrySet().iterator();
        //为了内部一点性能就不使用函数接口了
        //noinspection Java8CollectionRemoveIf
        while (iterator.hasNext()) {
            Map.Entry<String, V> next = iterator.next();
            if (predicate.test(next.getKey(), next.getValue())) {
                iterator.remove();
            }
        }
        return this;
    }
}
