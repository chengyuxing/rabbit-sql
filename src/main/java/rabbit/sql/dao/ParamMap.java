package rabbit.sql.dao;

import rabbit.sql.support.IOutParam;
import rabbit.sql.types.Param;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 参数构建器
 */
public class ParamMap extends HashMap<String, Param> {
    /**
     * 创建一个新的参数Map
     *
     * @return 新的参数Map
     */
    public static ParamMap create() {
        return new ParamMap();
    }

    /**
     * 返回一个空的不可变的参数Map
     *
     * @return 空
     */
    public static Map<String, Param> empty() {
        return Collections.emptyMap();
    }

    /**
     * 从一个map转换为Param值类型，全部为进参
     *
     * @param source 数据源
     * @return paramMap
     */
    public static ParamMap from(Map<String, Object> source) {
        return source.keySet().stream().collect(
                ParamMap::new,
                (current, k) -> current.put(k, Param.IN(source.get(k))),
                ParamMap::putAll
        );
    }

    /**
     * put入参
     *
     * @param name  键
     * @param value 值 (可以是明确的值或包装值)
     * @return paramMap
     * @see Wrap
     */
    public ParamMap putIn(String name, Object value) {
        put(name, Param.IN(value));
        return this;
    }

    /**
     * put出参
     *
     * @param name 键
     * @param type 出参类型
     * @return paramMap
     */
    public ParamMap putOut(String name, IOutParam type) {
        put(name, Param.OUT(type));
        return this;
    }

    /**
     * put出入参
     *
     * @param name  键
     * @param value 入参值 (可以是明确的值或包装值)
     * @param type  出参类型
     * @return paramMap
     * @see Wrap
     */
    public ParamMap putInOut(String name, Object value, IOutParam type) {
        put(name, Param.IN_OUT(value, type));
        return this;
    }

    /**
     * put sql字符串模版
     *
     * @param name     键
     * @param template sql模版
     * @return paramMap
     */
    public ParamMap putTemplate(String name, String template) {
        put(name, Param.TEMPLATE(template));
        return this;
    }
}
