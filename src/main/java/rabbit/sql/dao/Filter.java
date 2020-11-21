package rabbit.sql.dao;

import rabbit.sql.support.IFilter;

/**
 * 条件过滤器
 */
public final class Filter implements IFilter {
    private final String operator;    // 操作符
    private final String field;       // 字段名
    private final Object value;       // 字段值

    Filter(String field, String operator, Object value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    /**
     * 定义一个过滤器
     *
     * @param field    字段名
     * @param operator 操作符
     * @param value    字段值
     * @return 过滤器
     */
    public static Filter of(String field, String operator, Object value) {
        return new Filter(field, operator, value);
    }

    /**
     * 等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter eq(String field, Object value) {
        return of(field, " = ", value);
    }

    /**
     * 不等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter neq(String field, Object value) {
        return of(field, " != ", value);
    }

    /**
     * 大于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter gt(String field, Object value) {
        return of(field, " > ", value);
    }

    /**
     * 小于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter lt(String field, Object value) {
        return of(field, " < ", value);
    }

    /**
     * 大于等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter gtEq(String field, Object value) {
        return of(field, " >= ", value);
    }

    /**
     * 小于等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter ltEq(String field, Object value) {
        return of(field, " <= ", value);
    }

    /**
     * 模糊匹配
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter like(String field, Object value) {
        return of(field, " like ", value);
    }

    /**
     * 模糊匹配前面
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter startsWith(String field, Object value) {
        return like(field, value + "%");
    }

    /**
     * 模糊匹配后面
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter endsWith(String field, Object value) {
        return like(field, "%" + value);
    }

    /**
     * 模糊不匹配
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static Filter notLike(String field, Object value) {
        return of(field, " not like ", value);
    }

    /**
     * 非空
     *
     * @param field 字段名
     * @return 过滤器
     */
    public static Filter isNotNull(String field) {
        return of(field, " is not null ", IGNORE_VALUE);
    }

    /**
     * 空
     *
     * @param field 字段名
     * @return 过滤器
     */
    public static Filter isNull(String field) {
        return of(field, " is null ", IGNORE_VALUE);
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public String getOperator() {
        return operator;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
