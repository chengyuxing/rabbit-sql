package com.github.chengyuxing.sql.support;

import java.util.Map;

/**
 * 条件拼接接口
 */
public interface ICondition {
    /**
     * 获取条件拼装器中所有的参数
     *
     * @return 参数
     */
    Map<String, Object> getArgs();

    /**
     * 获取条件sql字符串
     *
     * @return sql
     */
    String getSql();
}
