package rabbit.sql.dao;

import rabbit.sql.types.Param;
import rabbit.sql.types.ValueWrap;
import rabbit.sql.support.IOutParam;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 参数工具类
 */
public class Params {
    /**
     * 参数构建器
     */
    public static class Builder {
        private final Map<String, Param> paramMap = new HashMap<>();

        /**
         * 同map.putAll
         *
         * @param params 参数
         * @return builder
         */
        public Builder putAll(Map<String, Param> params) {
            paramMap.putAll(params);
            return this;
        }

        /**
         * 同map.put
         *
         * @param name  键
         * @param param 值
         * @return builder
         */
        public Builder put(String name, Param param) {
            paramMap.put(name, param);
            return this;
        }

        /**
         * put入参
         *
         * @param name  键
         * @param value 值 (可以是明确的值或包装值)
         * @return builder
         * @see ValueWrap
         */
        public Builder putIn(String name, Object value) {
            put(name, Param.IN(value));
            return this;
        }

        /**
         * put出参
         *
         * @param name 键
         * @param type 出参类型
         * @return builder
         */
        public Builder putOut(String name, IOutParam type) {
            put(name, Param.OUT(type));
            return this;
        }

        /**
         * put出入参
         *
         * @param name  键
         * @param value 入参值 (可以是明确的值或包装值)
         * @param type  出参类型
         * @return builder
         * @see ValueWrap
         */
        public Builder putInOut(String name, Object value, IOutParam type) {
            put(name, Param.IN_OUT(value, type));
            return this;
        }

        /**
         * put sql字符串模版
         *
         * @param name     键
         * @param template sql模版
         * @return builder
         */
        public Builder putTemplate(String name, String template) {
            put(name, Param.TEMPLATE(template));
            return this;
        }

        /**
         * 创建
         *
         * @return 一组参数
         */
        public Map<String, Param> build() {
            return paramMap;
        }
    }

    /**
     * 构建器
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 返回一个空的参数Map
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
    public static Map<String, Param> from(Map<String, Object> source) {
        return source.keySet().stream().collect(
                HashMap::new,
                (current, k) -> current.put(k, Param.IN(source.get(k))),
                HashMap::putAll
        );
    }
}
