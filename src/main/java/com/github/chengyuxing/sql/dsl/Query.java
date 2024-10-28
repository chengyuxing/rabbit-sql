package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.dsl.clause.Where;
import com.github.chengyuxing.sql.dsl.type.ColumnReference;
import com.github.chengyuxing.sql.dsl.type.OrderByType;
import com.github.chengyuxing.sql.page.PageHelperProvider;
import com.github.chengyuxing.sql.utils.EntityUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.util.*;
import java.util.stream.Stream;

public abstract class Query<T> extends Where<T, Query<T>> {
    protected final List<Pair<String, OrderByType>> orders = new ArrayList<>();

    public Query(Class<T> entityClass) {
        super(entityClass);
    }

    public Query<T> orderBy(@NotNull String column, OrderByType order) {
        orders.add(Pair.of(column, order));
        return this;
    }

    public Query<T> orderBy(ColumnReference<T> column, OrderByType order) {
        return orderBy(EntityUtil.getFieldNameWithCache(column), order);
    }

    public Query<T> orderByAsc(@NotNull String column) {
        orders.add(Pair.of(column, OrderByType.ASC));
        return this;
    }

    public Query<T> orderByAsc(ColumnReference<T> column) {
        return orderByAsc(EntityUtil.getFieldNameWithCache(column));
    }

    public Query<T> orderByDesc(@NotNull String column) {
        orders.add(Pair.of(column, OrderByType.DESC));
        return this;
    }

    public Query<T> orderByDesc(ColumnReference<T> column) {
        return orderByDesc(EntityUtil.getFieldNameWithCache(column));
    }

    protected String buildOrders() {
        if (orders.isEmpty()) {
            return "";
        }
        StringJoiner joiner = new StringJoiner(", ");
        for (Pair<String, OrderByType> order : orders) {
            checkConditionField(order.getItem1());
            joiner.add(order.getItem1() + " " + order.getItem2().name().toLowerCase());
        }
        return "\norder by " + joiner;
    }

    public abstract Stream<T> toStream();

    public abstract List<T> toList();

    public abstract Optional<T> findFirst();

    public abstract @Nullable T getFirst();

    public abstract PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                     @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                                     @Nullable PageHelperProvider pageHelperProvider);

    public abstract PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                     @Range(from = 1, to = Integer.MAX_VALUE) int size);

    public abstract boolean exists();

    public abstract @NotNull DataRow findFirstRow();

    public abstract List<DataRow> toRows();

    public abstract List<Map<String, Object>> toMaps();
}
