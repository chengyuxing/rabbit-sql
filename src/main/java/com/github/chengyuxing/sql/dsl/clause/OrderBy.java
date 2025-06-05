package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.dsl.types.OrderByType;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Order by clause.
 *
 * @param <T> entity type
 */
public abstract class OrderBy<T> extends ColumnHelper<T> {
    protected final Set<Pair<String, OrderByType>> orders = new LinkedHashSet<>();

    /**
     * Construct a new Order by builder.
     *
     * @param clazz entity type
     */
    public OrderBy(@NotNull Class<T> clazz) {
        super(clazz);
    }

    /**
     * Order by.
     *
     * @param column column
     * @param order  order by type
     * @return order by builder
     */
    public OrderBy<T> by(@NotNull String column, OrderByType order) {
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
    public OrderBy<T> by(FieldReference<T> column, OrderByType order) {
        return by(getColumnName(column), order);
    }

    /**
     * {@code asc}
     *
     * @param column column
     * @return order by builder
     */
    public OrderBy<T> asc(FieldReference<T> column) {
        orders.add(Pair.of(getColumnName(column), OrderByType.ASC));
        return this;
    }

    /**
     * {@code desc}
     *
     * @param column column
     * @return order by builder
     */
    public OrderBy<T> desc(FieldReference<T> column) {
        orders.add(Pair.of(getColumnName(column), OrderByType.DESC));
        return this;
    }

    protected final String build() {
        if (orders.isEmpty()) {
            return "";
        }
        StringJoiner joiner = new StringJoiner(", ");
        for (Pair<String, OrderByType> order : orders) {
            if (isIllegalColumn(order.getItem1())) {
                throw new IllegalArgumentException("unexpected column: '" + order.getItem1() + "' on order by: " + order);
            }
            joiner.add(order.getItem1() + " " + order.getItem2().name().toLowerCase());
        }
        return "\norder by " + joiner;
    }
}
