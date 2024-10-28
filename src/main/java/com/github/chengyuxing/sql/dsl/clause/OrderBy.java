package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.type.ColumnReference;
import com.github.chengyuxing.sql.dsl.type.OrderByType;
import com.github.chengyuxing.sql.utils.EntityUtil;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class OrderBy<T> {
    protected final List<Pair<String, OrderByType>> orders = new ArrayList<>();

    public OrderBy<T> orderBy(@NotNull String column, OrderByType order) {
        orders.add(Pair.of(column, order));
        return this;
    }

    public OrderBy<T> orderBy(ColumnReference<T> column, OrderByType order) {
        return orderBy(EntityUtil.getFieldNameWithCache(column), order);
    }

    public OrderBy<T> orderByAsc(@NotNull String column) {
        orders.add(Pair.of(column, OrderByType.ASC));
        return this;
    }

    public OrderBy<T> orderByAsc(ColumnReference<T> column) {
        return orderByAsc(EntityUtil.getFieldNameWithCache(column));
    }

    public OrderBy<T> orderByDesc(@NotNull String column) {
        orders.add(Pair.of(column, OrderByType.DESC));
        return this;
    }

    public OrderBy<T> orderByDesc(ColumnReference<T> column) {
        return orderByDesc(EntityUtil.getFieldNameWithCache(column));
    }
}
