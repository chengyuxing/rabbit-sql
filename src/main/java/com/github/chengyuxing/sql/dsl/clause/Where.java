package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.type.ColumnReference;
import com.github.chengyuxing.sql.dsl.type.Operator;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.github.chengyuxing.sql.dsl.type.StandardOperator.*;

public abstract class Where<T> extends ColumnHelper<T> {
    protected List<Criteria> criteria = new ArrayList<>();

    /**
     * Construct a new Where builder.
     *
     * @param clazz entity class
     */
    public Where(@NotNull Class<T> clazz) {
        super(clazz);
    }

    /**
     * Construct a new Where builder with initial where builder.
     *
     * @param clazz entity class
     * @param other where builder
     */
    public Where(@NotNull Class<T> clazz, @NotNull Where<T> other) {
        super(clazz);
        this.criteria = other.criteria;
    }

    /**
     * Returns a new where builder.
     *
     * @return where builder
     */
    protected abstract Where<T> newInstance();

    protected abstract char namedParamPrefix();

    @SafeVarargs
    public final <E> Where<T> of(ColumnReference<T> column, @NotNull Operator operator, E value, Predicate<E>... predicates) {
        if (operator == LOGIC_OR || operator == LOGIC_AND) {
            throw new IllegalArgumentException("logic operator '" + operator.getValue() + "' invalid at this time");
        }
        if (isConditionMatched(value, predicates)) {
            addCondition(column, operator, value);
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> eq(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            if (Objects.nonNull(value)) {
                addCondition(column, EQ, value);
            } else {
                addCondition(column, IS_NULL, null);
            }
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> neq(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            if (Objects.nonNull(value)) {
                addCondition(column, NEQ, value);
            } else {
                addCondition(column, IS_NOT_NULL, null);
            }
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> gt(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, GT, value);
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> lt(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LT, value);
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> gte(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, GTE, value);
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> lte(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LTE, value);
        }
        return this;
    }

    @SafeVarargs
    public final <E, V extends Collection<E>> Where<T> in(ColumnReference<T> column, V values, Predicate<V>... predicates) {
        if (isConditionMatched(values, predicates)) {
            criteria.add(new InCondition<>(getColumnName(column), IN, values));
        }
        return this;
    }

    @SafeVarargs
    public final <E, V extends Collection<E>> Where<T> notIn(ColumnReference<T> column, V values, Predicate<V>... predicates) {
        if (isConditionMatched(values, predicates)) {
            criteria.add(new InCondition<>(getColumnName(column), NOT_IN, values));
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> between(ColumnReference<T> column, E a, E b, BiPredicate<E, E>... predicates) {
        if (isConditionMatched(a, b, predicates)) {
            criteria.add(new BetweenCondition(getColumnName(column), BETWEEN, Pair.of(a, b)));
        }
        return this;
    }

    @SafeVarargs
    public final <E> Where<T> notBetween(ColumnReference<T> column, E a, E b, BiPredicate<E, E>... predicates) {
        if (isConditionMatched(a, b, predicates)) {
            criteria.add(new BetweenCondition(getColumnName(column), NOT_BETWEEN, Pair.of(a, b)));
        }
        return this;
    }

    @SafeVarargs
    public final Where<T> like(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, "%" + value + "%");
        }
        return this;
    }

    @SafeVarargs
    public final Where<T> notLike(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, "%" + value + "%");
        }
        return this;
    }

    @SafeVarargs
    public final Where<T> startsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, value + "%");
        }
        return this;
    }

    @SafeVarargs
    public final Where<T> notStartsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, value + "%");
        }
        return this;
    }

    @SafeVarargs
    public final Where<T> endsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, "%" + value);
        }
        return this;
    }

    @SafeVarargs
    public final Where<T> notEndsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, "%" + value);
        }
        return this;
    }

    public Where<T> isNull(ColumnReference<T> column, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            addCondition(column, IS_NULL, null);
        }
        return this;
    }

    public Where<T> isNotNull(ColumnReference<T> column, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            addCondition(column, IS_NOT_NULL, null);
        }
        return this;
    }

    public Where<T> and(Function<Where<T>, Where<T>> andGroup) {
        List<Criteria> criteriaList = andGroup.apply(newInstance()).criteria;
        criteria.add(new AndGroup(criteriaList));
        return this;
    }

    public Where<T> or(Function<Where<T>, Where<T>> orGroup) {
        List<Criteria> criteriaList = orGroup.apply(newInstance()).criteria;
        criteria.add(new OrGroup(criteriaList));
        return this;
    }

    /**
     * Returns a where condition consisting of the where builder, check the built result currently.
     *
     * @param consumer built result consumer (sql, (name parameter sql, params)) -&gt; _
     * @return self
     */
    public Where<T> peek(BiConsumer<String, Pair<String, Map<String, Object>>> consumer) {
        Pair<String, Map<String, Object>> where = build();
        String sql = new SqlGenerator(namedParamPrefix()).generateSql(where.getItem1(), where.getItem2());
        consumer.accept(sql, where);
        return this;
    }

    /**
     * Returns where clause and params.
     *
     * @return where clause and params.
     */
    protected @NotNull @Unmodifiable Pair<String, Map<String, Object>> build() {
        Pair<String, Map<String, Object>> where = build(new AtomicInteger(0), criteria, LOGIC_AND.padWithSpace(), 0);
        if (!where.getItem1().isEmpty()) {
            return Pair.of("\nwhere " + where.getItem1(), Collections.unmodifiableMap(where.getItem2()));
        }
        return where;
    }

    private void addCondition(ColumnReference<T> column, Operator operator, Object value) {
        criteria.add(new Condition<>(getColumnName(column), operator, value));
    }

    private static boolean isConditionMatched(boolean... predicates) {
        boolean matched = true;
        for (boolean predicate : predicates) {
            if (!predicate) {
                matched = false;
                break;
            }
        }
        return matched;
    }

    @SafeVarargs
    private static <E> boolean isConditionMatched(E value, Predicate<E>... predicates) {
        boolean matched = true;
        for (Predicate<E> predicate : predicates) {
            if (!predicate.test(value)) {
                matched = false;
                break;
            }
        }
        return matched;
    }

    @SafeVarargs
    private static <E> boolean isConditionMatched(E a, E b, BiPredicate<E, E>... predicates) {
        boolean matched = true;
        for (BiPredicate<E, E> predicate : predicates) {
            if (!predicate.test(a, b)) {
                matched = false;
                break;
            }
        }
        return matched;
    }

    protected final @NotNull @Unmodifiable Pair<String, Map<String, Object>> build(AtomicInteger uniqueIndex, List<Criteria> criteriaList, String logicOperator, int identLevel) {
        StringBuilder where = new StringBuilder();
        String ident = StringUtil.repeat("    ", identLevel);
        Map<String, Object> params = new HashMap<>();
        for (int i = 0, j = criteriaList.size(); i < j; i++) {
            Criteria criteria = criteriaList.get(i);
            if (criteria instanceof Condition) {
                @SuppressWarnings("unchecked") Condition<Object> condition = (Condition<Object>) criteria;

                if (isIllegalColumn(condition.getColumn())) {
                    throw new IllegalArgumentException("unexpected column: '" + condition.getColumn() + "' on condition: " + condition);
                }

                if (isIllegalOperator(condition.getOperator())) {
                    throw new IllegalArgumentException("Illegal operator: '" + condition.getOperator().getValue() + "' at condition: " + condition + ", add trusted operator into operatorWhiteList");
                }

                int unique = uniqueIndex.getAndIncrement();

                if (condition instanceof InCondition) {
                    @SuppressWarnings({"rawtypes", "unchecked"}) Pair<String, Map<String, Object>> result = ((InCondition) condition).buildStatement(unique, namedParamPrefix());
                    where.append(result.getItem1());
                    params.putAll(result.getItem2());
                } else if (condition instanceof BetweenCondition) {
                    Pair<String, Map<String, Object>> result = ((BetweenCondition) condition).buildStatement(unique, namedParamPrefix());
                    where.append(result.getItem1());
                    params.putAll(result.getItem2());
                } else {
                    if (condition.getOperator() == IS_NULL || condition.getOperator() == IS_NOT_NULL) {
                        where.append(condition.getColumn())
                                .append(" ")
                                .append(condition.getOperator().getValue());
                    } else {
                        String key = condition.getKey(unique);
                        where.append(condition.getColumn())
                                .append(condition.getOperator().padWithSpace())
                                .append(namedParamPrefix()).append(key);
                        params.put(key, condition.getValue());
                    }
                }
            } else if (criteria instanceof AndGroup) {
                List<Criteria> andGroup = ((AndGroup) criteria).getGroup();
                if (!andGroup.isEmpty()) {
                    Pair<String, Map<String, Object>> result = build(uniqueIndex, andGroup, LOGIC_AND.padWithSpace(), identLevel + 1);
                    where.append("(").append(result.getItem1()).append(")");
                    params.putAll(result.getItem2());
                }
            } else if (criteria instanceof OrGroup) {
                List<Criteria> orGroup = ((OrGroup) criteria).getGroup();
                if (!orGroup.isEmpty()) {
                    Pair<String, Map<String, Object>> result = build(uniqueIndex, orGroup, LOGIC_OR.padWithSpace(), identLevel + 1);
                    where.append("(").append(result.getItem1()).append(")");
                    params.putAll(result.getItem2());
                }
            }

            if (i < j - 1) {
                where.append("\n").append(ident);
                Criteria next = criteriaList.get(i + 1);
                // set current logical operator where the next 'and' and 'or' group invoked
                if (next instanceof AndGroup) {
                    where.append(LOGIC_AND.padWithSpace());
                } else if (next instanceof OrGroup) {
                    where.append(LOGIC_OR.padWithSpace());
                } else {
                    where.append(logicOperator);
                }
            }
        }
        return Pair.of(where.toString(), params);
    }
}
