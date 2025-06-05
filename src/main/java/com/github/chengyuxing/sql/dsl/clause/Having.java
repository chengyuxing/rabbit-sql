package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.dsl.types.Logic;
import com.github.chengyuxing.sql.dsl.types.StandardAggFunction;
import com.github.chengyuxing.sql.dsl.types.StandardOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Group by having clause builder.
 *
 * @param <T> entity type
 */
public abstract class Having<T> extends CriteriaBuilder<T> {
    protected final List<Criteria> criteria = new ArrayList<>();

    /**
     * Construct a new Having builder.
     *
     * @param clazz entity class
     */
    protected Having(@NotNull Class<T> clazz) {
        super(clazz);
    }

    /**
     * Returns a new Having builder.
     *
     * @return having builder
     */
    protected abstract Having<T> newInstance();

    /**
     * {@code count(*)}
     *
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator}
     * @param value    value
     * @return having builder
     */
    public Having<T> count(StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.COUNT, "*", operator, value);
        return this;
    }

    /**
     * {@code count([column])}
     *
     * @param column   column
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator}
     * @param value    value
     * @return having builder
     */
    public Having<T> count(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.COUNT, getColumnName(column), operator, value);
        return this;
    }

    /**
     * {@code sum([column])}
     *
     * @param column   column
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator}
     * @param value    value
     * @return having builder
     */
    public Having<T> sum(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.SUM, getColumnName(column), operator, value);
        return this;
    }

    /**
     * {@code avg([column])}
     *
     * @param column   column
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator}
     * @param value    value
     * @return having builder
     */
    public Having<T> avg(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.AVG, getColumnName(column), operator, value);
        return this;
    }

    /**
     * {@code min([column])}
     *
     * @param column   column
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator}
     * @param value    value
     * @return having builder
     */
    public Having<T> min(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.MIN, getColumnName(column), operator, value);
        return this;
    }

    /**
     * {@code max([column])}
     *
     * @param column   column
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator}
     * @param value    value
     * @return having builder
     */
    public Having<T> max(FieldReference<T> column, StandardOperator operator, Object value) {
        addCondition(StandardAggFunction.MAX, getColumnName(column), operator, value);
        return this;
    }

    /**
     * And group, all condition will be concat with {@code or}, {@code and (...or...or...or...)}
     *
     * @param andGroup and group
     * @return having builder
     * @see Where#and(Function, boolean...)
     */
    public Having<T> and(Function<Having<T>, Having<T>> andGroup) {
        List<Criteria> criteriaList = andGroup.apply(newInstance()).criteria;
        criteria.add(new AndGroup(criteriaList));
        return this;
    }

    /**
     * Or group, all condition will be concat with {@code and}, {@code or (...and...and...and...)}
     *
     * @param orGroup or group
     * @return having builder
     * @see Where#or(Function, boolean...)
     */
    public Having<T> or(Function<Having<T>, Having<T>> orGroup) {
        List<Criteria> criteriaList = orGroup.apply(newInstance()).criteria;
        criteria.add(new OrGroup(criteriaList));
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
        String valueKey = column.equals("*") ? "all" : column;
        if (operator == StandardOperator.BETWEEN || operator == StandardOperator.NOT_BETWEEN) {
            if (value instanceof Pair) {
                //noinspection unchecked
                criteria.add(new BetweenCondition<>(aggColumn, operator, (Pair<Object, Object>) value, valueKey));
                return;
            }
            Object[] arr = ObjectUtil.toArray(value);
            if (arr.length != 2) {
                throw new IllegalArgumentException("between operator must takes two values");
            }
            criteria.add(new BetweenCondition<>(aggColumn, operator, Pair.of(arr[0], arr[1]), valueKey));
            return;
        }
        if (operator == StandardOperator.IN || operator == StandardOperator.NOT_IN) {
            criteria.add(new InCondition<>(aggColumn, operator, Arrays.asList(ObjectUtil.toArray(value)), valueKey));
            return;
        }
        criteria.add(new Condition<>(aggColumn, operator, value, valueKey));
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
