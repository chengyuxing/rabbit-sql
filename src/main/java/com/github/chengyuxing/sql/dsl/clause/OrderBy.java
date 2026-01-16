package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.MethodReference;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.types.OrderByType;
import com.github.chengyuxing.sql.utils.SqlUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Order by clause.
 *
 * @param <T> entity type
 */
public abstract class OrderBy<T> {
    protected final Set<Pair<String, OrderByType>> orders = new LinkedHashSet<>();

    public OrderBy() {
    }

    protected OrderBy(OrderBy<T> orderBy) {
        this.orders.addAll(orderBy.orders);
    }

    protected abstract @NotNull String getColumnName(@NotNull MethodReference<T> fieldReference);

    /**
     * Order by.
     *
     * @param column column
     * @param order  order by type
     * @return order by builder
     */
    public OrderBy<T> by(@NotNull String column, OrderByType order) {
        SqlUtils.assertInvalidIdentifier(column);
        orders.add(Pair.of(column, order));
        return this;
    }

    /**
     * Order by.
     *
     * @param column column
     * @param order  order by type
     * @return order by builder
     */
    public OrderBy<T> by(MethodReference<T> column, OrderByType order) {
        return by(getColumnName(column), order);
    }

    /**
     * {@code asc}
     *
     * @param column column
     * @return order by builder
     */
    public OrderBy<T> asc(MethodReference<T> column) {
        orders.add(Pair.of(getColumnName(column), OrderByType.ASC));
        return this;
    }

    /**
     * {@code desc}
     *
     * @param column column
     * @return order by builder
     */
    public OrderBy<T> desc(MethodReference<T> column) {
        orders.add(Pair.of(getColumnName(column), OrderByType.DESC));
        return this;
    }

    protected final String build() {
        if (orders.isEmpty()) {
            return "";
        }
        StringJoiner joiner = new StringJoiner(", ");
        for (Pair<String, OrderByType> order : orders) {
            joiner.add(order.getItem1() + " " + order.getItem2().name().toLowerCase());
        }
        return "\norder by " + joiner;
    }
}
