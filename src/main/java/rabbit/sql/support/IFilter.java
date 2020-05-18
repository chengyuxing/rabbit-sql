package rabbit.sql.support;

import rabbit.sql.utils.SqlUtil;

/**
 * 条件过滤器通用接口
 */
public interface IFilter {
    /**
     * 可被忽略的值
     */
    String IGNORE_VALUE = "__IGN" + SqlUtil.SEP + "ORE__";

    /**
     * 获取字段名
     *
     * @return 字段名
     */
    String getField();

    /**
     * 获取操作符
     *
     * @return 操作符
     */
    String getOperator();

    /**
     * 值，如果返回 "IGNORE_VALUE" 则忽略
     *
     * @return 值
     */
    Object getValue();
}
