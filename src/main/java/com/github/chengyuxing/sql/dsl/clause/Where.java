package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.dsl.types.Logic;
import com.github.chengyuxing.sql.dsl.types.Operator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
    protected final List<Criteria> criteria = new ArrayList<>();

    /**
     * Construct a new Where builder.
     *
     * @param clazz entity class
     */
    public Where(@NotNull Class<T> clazz) {
        super(clazz);
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
     * Add a condition as the identity, the value is in the entity.
     *
     * @param column   column
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator} or other trusted operator
     * @return where builder
     */
    public Where<T> identity(FieldReference<T> column, @NotNull Operator operator) {
        if (operator == Logic.AND || operator == Logic.OR) {
            throw new IllegalArgumentException("logic operator '" + operator.getValue() + "' invalid at this time");
        }
        addCondition(column, operator, IDENTITY);
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
     * @param <E>        value item type
     * @param <V>        value type
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
     * @param <E>        value item type
     * @param <V>        value type
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
            criteria.add(new BetweenCondition<>(getColumnName(column), BETWEEN, Pair.of(a, b)));
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
            criteria.add(new BetweenCondition<>(getColumnName(column), NOT_BETWEEN, Pair.of(a, b)));
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
    public final Where<T> like(FieldReference<T> column, String value, Predicate<String>... predicates) {
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
    public final Where<T> notLike(FieldReference<T> column, String value, Predicate<String>... predicates) {
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
    public final Where<T> startsWith(FieldReference<T> column, String value, Predicate<String>... predicates) {
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
    public final Where<T> notStartsWith(FieldReference<T> column, String value, Predicate<String>... predicates) {
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
    public final Where<T> endsWith(FieldReference<T> column, String value, Predicate<String>... predicates) {
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
    public final Where<T> notEndsWith(FieldReference<T> column, String value, Predicate<String>... predicates) {
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
     * w -&gt; w.and(o -&gt; o.or(a -&gt; a.eq(Guest::getName, "cyx")
     *                          .eq(Guest::getAge, 30))
     *                  .or(r -&gt; r.eq(Guest::getName, "jack")
     *                          .eq(Guest::getAge, 60))
     *          )
     * </pre></blockquote>
     *
     * @param orGroup    and group
     * @param predicates all predicates to the true add the group otherwise avoid the group.
     * @return where builder
     * @see #or(Function, boolean...)
     */
    public Where<T> and(Function<Where<T>, Where<T>> orGroup, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            List<Criteria> criteriaList = orGroup.apply(newInstance()).criteria;
            criteria.add(new AndGroup(criteriaList));
        }
        return this;
    }

    /**
     * Or group, all condition will be concat with {@code and}, {@code or (...and...ang...and...)}<br>
     * E.g. simple nest condition:
     * <blockquote><pre>
     * {@code (age &lt; 15 or age &gt; 60) and name = 'cyx'}
     * </pre></blockquote>
     * <blockquote><pre>
     * w -&gt; w.and(o -&gt; o.lt(Guest::getAge, 15)
     *               .gt(Guest::getAge, 60))
     *       .eq(Guest::getName, "cyx")
     * </pre></blockquote>
     *
     * @param andGroup   or group
     * @param predicates all predicates to the true add the group otherwise avoid the group.
     * @return where builder
     * @see #and(Function, boolean...)
     */
    public Where<T> or(Function<Where<T>, Where<T>> andGroup, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            List<Criteria> criteriaList = andGroup.apply(newInstance()).criteria;
            criteria.add(new OrGroup(criteriaList));
        }
        return this;
    }

    /**
     * Returns where clause and params.
     *
     * @return where clause, params, identity columns.
     */
    protected @NotNull @Unmodifiable Triple<String, Map<String, Object>, Set<String>> build() {
        Triple<String, Map<String, Object>, Set<String>> where = build(new AtomicInteger(0), criteria, Logic.AND, 0);
        if (!where.getItem1().isEmpty()) {
            return Triple.of("\nwhere " + where.getItem1(), Collections.unmodifiableMap(where.getItem2()), Collections.unmodifiableSet(where.getItem3()));
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
