package rabbit.sql.dao;

import rabbit.sql.support.IOrderBy;
import rabbit.sql.types.Order;
import rabbit.sql.types.Param;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 排序
 */
public final class OrderBy implements IOrderBy {
    private final List<String> orders = new ArrayList<>();

    OrderBy() {
    }

    @Override
    public OrderBy orderBy(String field, Order order) {
        orders.add(field + " " + order.getValue());
        return this;
    }

    @Override
    public Map<String, Param> getParams() {
        return Params.empty();
    }

    @Override
    public String getSql() {
        return "\n order by " + String.join(", ", orders);
    }
}
