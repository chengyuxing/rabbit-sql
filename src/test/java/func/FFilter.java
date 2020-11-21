package func;

import java.lang.reflect.InvocationTargetException;

public class FFilter<T> {
    private final String operator;    // 操作符
    private final FieldFunc<T> field;       // 字段名
    private final Object value;       // 字段值

    FFilter(FieldFunc<T> field, String operator, Object value) {
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
    public static <T> FFilter<T> of(FieldFunc<T> field, String operator, Object value) {
        return new FFilter<>(field, operator, value);
    }

    /**
     * 等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> eq(FieldFunc<T> field, Object value) {
        return of(field, " = ", value);
    }

    /**
     * 不等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> neq(FieldFunc<T> field, Object value) {
        return of(field, " != ", value);
    }

    /**
     * 大于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> gt(FieldFunc<T> field, Object value) {
        return of(field, " > ", value);
    }

    /**
     * 小于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> lt(FieldFunc<T> field, Object value) {
        return of(field, " < ", value);
    }

    /**
     * 大于等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> gtEq(FieldFunc<T> field, Object value) {
        return of(field, " >= ", value);
    }

    /**
     * 小于等于
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> ltEq(FieldFunc<T> field, Object value) {
        return of(field, " <= ", value);
    }

    /**
     * 模糊匹配
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> like(FieldFunc<T> field, Object value) {
        return of(field, " like ", value);
    }

    /**
     * 模糊匹配前面
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> startsWith(FieldFunc<T> field, Object value) {
        return like(field, value + "%");
    }

    /**
     * 模糊匹配后面
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> endsWith(FieldFunc<T> field, Object value) {
        return like(field, "%" + value);
    }

    /**
     * 模糊不匹配
     *
     * @param field 字段名
     * @param value 字段值
     * @return 过滤器
     */
    public static <T> FFilter<T> notLike(FieldFunc<T> field, Object value) {
        return of(field, " not like ", value);
    }

    public String getField() {
        try {
            return BeanUtil.convert2fieldName(field);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            return "";
        }
    }

    public String getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }
}
