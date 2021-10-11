package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.types.Param;

import java.util.HashMap;

/**
 * sql参数构建工具类
 *
 * @param <V> 类型参数
 */
public class Args<V> extends HashMap<String, V> {
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
        if (pairOfArgs.length == 0 || pairOfArgs.length % 2 != 0) {
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
     * 添加字符串模版类型参数
     * <blockquote>
     * e.g. 默认使用'${}'包裹
     * <pre>${key}</pre>
     * </blockquote>
     *
     * @param key   键名
     * @param value 值
     * @return 参数集合
     */
    public Args<V> addStrTemplate(String key, V value) {
        return add("${" + key + "}", value);
    }
}
