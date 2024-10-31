package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.type.ColumnReference;
import com.github.chengyuxing.sql.dsl.type.OrderByType;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public abstract class OrderBy<T> extends ColumnHelper<T> {
    protected Set<Pair<String, OrderByType>> orders = new LinkedHashSet<>();

    public OrderBy(@NotNull Class<T> clazz) {
        super(clazz);
    }

    public OrderBy(@NotNull Class<T> clazz, @NotNull OrderBy<T> other) {
        super(clazz);
        this.orders = other.orders;
    }

    public OrderBy<T> by(@NotNull String column, OrderByType order) {
        orders.add(Pair.of(column, order));
        return this;
    }

    public OrderBy<T> by(ColumnReference<T> column, OrderByType order) {
        return by(getColumnName(column), order);
    }

    public OrderBy<T> asc(@NotNull String column) {
        orders.add(Pair.of(column, OrderByType.ASC));
        return this;
    }

    public OrderBy<T> asc(ColumnReference<T> column) {
        return asc(getColumnName(column));
    }

    public OrderBy<T> desc(@NotNull String column) {
        orders.add(Pair.of(column, OrderByType.DESC));
        return this;
    }

    public OrderBy<T> desc(ColumnReference<T> column) {
        return desc(getColumnName(column));
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
