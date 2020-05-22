package rabbit.sql.support;

import rabbit.sql.types.Order;

/**
 * 排序接口
 */
public interface IOrderBy extends ICondition {
    /**
     * 根据指定字段排序
     *
     * @param field 字段名
     * @param order 排序方式
     * @return orderIy
     */
    IOrderBy orderBy(String field, Order order);

    /**
     * 正序
     *
     * @param field 字段名
     * @return 正序
     */
    default IOrderBy asc(String field) {
        return orderBy(field, Order.ASC);
    }

    /**
     * 倒序
     *
     * @param field 字段名
     * @return 倒序
     */
    default IOrderBy desc(String field) {
        return orderBy(field, Order.DESC);
    }
}
