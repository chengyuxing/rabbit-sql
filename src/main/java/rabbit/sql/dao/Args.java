package rabbit.sql.dao;

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
}
