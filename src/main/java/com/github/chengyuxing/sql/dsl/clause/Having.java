package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.type.FieldReference;
import com.github.chengyuxing.sql.dsl.type.Logic;
import com.github.chengyuxing.sql.dsl.type.StandardAggFunction;
import com.github.chengyuxing.sql.dsl.type.StandardOperator;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

public abstract class Having<T> extends CriteriaBuilder<T> {
    protected List<Criteria> criteria = new ArrayList<>();

    protected Having(@NotNull Class<T> clazz) {
        super(clazz);
    }

    protected Having(@NotNull Class<T> clazz, Having<T> other) {
        super(clazz);
        this.criteria = other.criteria;
    }

    protected abstract Having<T> newInstance();

    public Having<T> count(StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.COUNT, "*", operator, value);
        return this;
    }

    public Having<T> count(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.COUNT, getColumnName(column), operator, value);
        return this;
    }

    public Having<T> sum(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.SUM, getColumnName(column), operator, value);
        return this;
    }

    public Having<T> avg(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.AVG, getColumnName(column), operator, value);
        return this;
    }

    public Having<T> min(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.MIN, getColumnName(column), operator, value);
        return this;
    }

    public Having<T> max(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.MAX, getColumnName(column), operator, value);
        return this;
    }

    public Having<T> and(Function<Having<T>, Having<T>> andGroup) {
        List<Criteria> criteriaList = andGroup.apply(newInstance()).criteria;
        criteria.add(new AndGroup(criteriaList));
        return this;
    }

    public Having<T> or(Function<Having<T>, Having<T>> orGroup) {
        List<Criteria> criteriaList = orGroup.apply(newInstance()).criteria;
        criteria.add(new OrGroup(criteriaList));
        return this;
    }

    /**
     * Returns a having condition consisting of the where builder, check the built result currently.
     *
     * @param consumer built result consumer (sql, (name parameter sql, params)) -&gt; _
     * @return self
     */
    public Having<T> peek(BiConsumer<String, Pair<String, Map<String, Object>>> consumer) {
        Pair<String, Map<String, Object>> where = build();
        String sql = new SqlGenerator(namedParamPrefix()).generateSql(where.getItem1(), where.getItem2());
        consumer.accept(sql, where);
        return this;
    }

    protected @NotNull @Unmodifiable Pair<String, Map<String, Object>> build() {
        Pair<String, Map<String, Object>> having = build(new AtomicInteger(1000), criteria, Logic.AND, 0);
        if (!having.getItem1().isEmpty()) {
            return Pair.of("\nhaving " + having.getItem1(), Collections.unmodifiableMap(having.getItem2()));
        }
        return having;
    }

    private void addCondition(StandardAggFunction agg, String column, StandardOperator operator, Object value) {
        String aggColumn = agg.apply(column);
        if (operator == StandardOperator.BETWEEN || operator == StandardOperator.NOT_BETWEEN) {
            Object[] arr = ObjectUtil.toArray(value);
            if (arr.length != 2) {
                throw new IllegalArgumentException("between operator must takes two values");
            }
            criteria.add(new BetweenCondition(aggColumn, operator, Pair.of(arr[0], arr[1]), column));
            return;
        }
        if (operator == StandardOperator.IN || operator == StandardOperator.NOT_IN) {
            criteria.add(new InCondition<>(aggColumn, operator, Arrays.asList(ObjectUtil.toArray(value)), column));
            return;
        }
        criteria.add(new Condition<>(aggColumn, operator, value, column));
    }

    @Override
    protected void doCheck(Condition<Object> condition) {
        // Unnecessary check.
    }

    @Override
    protected Set<String> columnWhiteList() {
        return Collections.emptySet();
    }

    @Override
    protected Set<String> operatorWhiteList() {
        return Collections.emptySet();
    }
}
