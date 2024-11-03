package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.dsl.types.Logic;
import com.github.chengyuxing.sql.dsl.types.Operator;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.github.chengyuxing.sql.dsl.types.StandardOperator.*;

/**
 * Where clause builder.
 *
 * @param <T> entity type
 */
public abstract class Where<T> extends CriteriaBuilder<T> {
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
     * Construct a new Where builder with initial Where builder.
     *
     * @param clazz entity class
     * @param other where builder
     */
    public Where(@NotNull Class<T> clazz, @NotNull Where<T> other) {
        super(clazz);
        this.criteria = other.criteria;
    }

    /**
     * Returns a new Where builder.
     *
     * @return where builder
     */
    protected abstract Where<T> newInstance();

    /**
     * Add a condition.
     *
     * @param column     column
     * @param operator   {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator} or other trusted operator
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> of(FieldReference<T> column, @NotNull Operator operator, E value, Predicate<E>... predicates) {
        if (operator == Logic.AND || operator == Logic.OR) {
            throw new IllegalArgumentException("logic operator '" + operator.getValue() + "' invalid at this time");
        }
        if (isConditionMatched(value, predicates)) {
            addCondition(column, operator, value);
        }
        return this;
    }

    /**
     * {@code =}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> eq(FieldReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            if (Objects.nonNull(value)) {
                addCondition(column, EQ, value);
            } else {
                addCondition(column, IS_NULL, null);
            }
        }
        return this;
    }

    /**
     * {@code <>}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> neq(FieldReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            if (Objects.nonNull(value)) {
                addCondition(column, NEQ, value);
            } else {
                addCondition(column, IS_NOT_NULL, null);
            }
        }
        return this;
    }

    /**
     * {@code >}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> gt(FieldReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, GT, value);
        }
        return this;
    }

    /**
     * {@code <}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> lt(FieldReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LT, value);
        }
        return this;
    }

    /**
     * {@code >=}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> gte(FieldReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, GTE, value);
        }
        return this;
    }

    /**
     * {@code <=}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> lte(FieldReference<T> column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LTE, value);
        }
        return this;
    }

    /**
     * {@code in (...)}
     *
     * @param column     column
     * @param values     {@link Collection} or Array.
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E, V extends Collection<E>> Where<T> in(FieldReference<T> column, V values, Predicate<V>... predicates) {
        if (isConditionMatched(values, predicates)) {
            criteria.add(new InCondition<>(getColumnName(column), IN, values));
        }
        return this;
    }

    /**
     * {@code not in (...)}
     *
     * @param column     column
     * @param values     {@link Collection} or Array.
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E, V extends Collection<E>> Where<T> notIn(FieldReference<T> column, V values, Predicate<V>... predicates) {
        if (isConditionMatched(values, predicates)) {
            criteria.add(new InCondition<>(getColumnName(column), NOT_IN, values));
        }
        return this;
    }

    /**
     * {@code between} a {@code and} b
     *
     * @param column     column
     * @param a          value 1.
     * @param b          value 2.
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> between(FieldReference<T> column, E a, E b, BiPredicate<E, E>... predicates) {
        if (isConditionMatched(a, b, predicates)) {
            criteria.add(new BetweenCondition(getColumnName(column), BETWEEN, Pair.of(a, b)));
        }
        return this;
    }

    /**
     * {@code not between} a {@code and} b
     *
     * @param column     column
     * @param a          value 1.
     * @param b          value 2.
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @param <E>        value type
     * @return where builder
     */
    @SafeVarargs
    public final <E> Where<T> notBetween(FieldReference<T> column, E a, E b, BiPredicate<E, E>... predicates) {
        if (isConditionMatched(a, b, predicates)) {
            criteria.add(new BetweenCondition(getColumnName(column), NOT_BETWEEN, Pair.of(a, b)));
        }
        return this;
    }

    /**
     * {@code like '%} str {@code %'}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    @SafeVarargs
    public final Where<T> like(FieldReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, "%" + value + "%");
        }
        return this;
    }

    /**
     * {@code not like '%} str {@code %'}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    @SafeVarargs
    public final Where<T> notLike(FieldReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, "%" + value + "%");
        }
        return this;
    }

    /**
     * {@code like '} str {@code %'}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    @SafeVarargs
    public final Where<T> startsWith(FieldReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, value + "%");
        }
        return this;
    }

    /**
     * {@code not like '} str {@code %'}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    @SafeVarargs
    public final Where<T> notStartsWith(FieldReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, value + "%");
        }
        return this;
    }

    /**
     * {@code like '%} str {@code '}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    @SafeVarargs
    public final Where<T> endsWith(FieldReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, "%" + value);
        }
        return this;
    }

    /**
     * {@code not like '%} str {@code '}
     *
     * @param column     column
     * @param value      value
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    @SafeVarargs
    public final Where<T> notEndsWith(FieldReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, "%" + value);
        }
        return this;
    }

    /**
     * {@code is null}
     *
     * @param column     column
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    public Where<T> isNull(FieldReference<T> column, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            addCondition(column, IS_NULL, null);
        }
        return this;
    }

    /**
     * {@code is not null}
     *
     * @param column     column
     * @param predicates all predicates to the true add the condition otherwise avoid the condition.
     * @return where builder
     */
    public Where<T> isNotNull(FieldReference<T> column, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            addCondition(column, IS_NOT_NULL, null);
        }
        return this;
    }

    /**
     * And group, all condition will be concat with {@code or}, {@code and (...or...or...or...)}<br>
     * E.g. the complex nest condition:
     * <blockquote><pre>
     * ((name = 'cyx' and age = 30) or (name = 'jack' and age = 60))
     * </pre></blockquote>
     * The built struct:
     * <blockquote><pre>
     * w -> w.and(o -> o.or(a -> a.eq(Guest::getName, "cyx")
     *                          .eq(Guest::getAge, 30))
     *                  .or(r -> r.eq(Guest::getName, "jack")
     *                          .eq(Guest::getAge, 60))
     *          )
     * </pre></blockquote>
     *
     * @param orGroup and group
     * @return where builder
     * @see #or(Function)
     */
    public Where<T> and(Function<Where<T>, Where<T>> orGroup) {
        List<Criteria> criteriaList = orGroup.apply(newInstance()).criteria;
        criteria.add(new AndGroup(criteriaList));
        return this;
    }

    /**
     * Or group, all condition will be concat with {@code and}, {@code or (...and...ang...and...)}<br>
     * E.g. simple nest condition:
     * <blockquote><pre>
     * {@code (age < 15 or age > 60) and name = 'cyx'}
     * </pre></blockquote>
     * <blockquote><pre>
     * w -> w.and(o -> o.lt(Guest::getAge, 15)
     *               .gt(Guest::getAge, 60))
     *       .eq(Guest::getName, "cyx")
     * </pre></blockquote>
     *
     * @param andGroup or group
     * @return where builder
     * @see #and(Function)
     */
    public Where<T> or(Function<Where<T>, Where<T>> andGroup) {
        List<Criteria> criteriaList = andGroup.apply(newInstance()).criteria;
        criteria.add(new OrGroup(criteriaList));
        return this;
    }

    /**
     * Returns a where condition consisting of the where builder, check the built result currently.
     *
     * @param consumer built result consumer (sql, (name parameter sql, params)) -&gt; _
     * @return where builder
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
        Pair<String, Map<String, Object>> where = build(new AtomicInteger(0), criteria, Logic.AND, 0);
        if (!where.getItem1().isEmpty()) {
            return Pair.of("\nwhere " + where.getItem1(), Collections.unmodifiableMap(where.getItem2()));
        }
        return where;
    }

    private void addCondition(FieldReference<T> column, Operator operator, Object value) {
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
}
