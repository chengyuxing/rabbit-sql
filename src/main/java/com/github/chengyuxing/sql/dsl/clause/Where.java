package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.MethodReference;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.types.Logic;
import com.github.chengyuxing.sql.dsl.types.Operator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.github.chengyuxing.sql.dsl.types.StandardOperator.*;

/**
 * Where clause builder.
 *
 * @param <T> entity type
 */
public abstract class Where<T> {
    protected final List<Criteria> criteria = new ArrayList<>();

    public Where() {
    }

    protected Where(Where<T> where) {
        this.criteria.addAll(where.criteria);
    }

    /**
     * Returns a new Where builder.
     *
     * @return where builder
     */
    protected abstract Where<T> newInstance();

    protected abstract char namedParamPrefix();

    protected abstract @NotNull String getColumnName(@NotNull MethodReference<T> MethodReference);

    /**
     * Add a condition.
     *
     * @param column   column
     * @param operator {@link com.github.chengyuxing.sql.dsl.types.StandardOperator StandardOperator} or other trusted operator
     * @param value    value
     * @param <E>      value type
     * @return where builder
     */
    public <E> Where<T> of(MethodReference<T> column, @NotNull Operator operator, E value) {
        if (operator == Logic.AND || operator == Logic.OR) {
            throw new IllegalArgumentException("logic operator '" + operator.getValue() + "' invalid at this time");
        }
        addCondition(column, operator, value);
        return this;
    }

    /**
     * {@code =}
     *
     * @param column column
     * @param value  value
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> eq(MethodReference<T> column, E value) {
        if (Objects.nonNull(value)) {
            addCondition(column, EQ, value);
        } else {
            addCondition(column, IS_NULL, null);
        }
        return this;
    }

    /**
     * {@code <>}
     *
     * @param column column
     * @param value  value
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> neq(MethodReference<T> column, E value) {
        if (Objects.nonNull(value)) {
            addCondition(column, NEQ, value);
        } else {
            addCondition(column, IS_NOT_NULL, null);
        }
        return this;
    }

    /**
     * {@code >}
     *
     * @param column column
     * @param value  value
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> gt(MethodReference<T> column, E value) {
        addCondition(column, GT, value);
        return this;
    }

    /**
     * {@code <}
     *
     * @param column column
     * @param value  value
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> lt(MethodReference<T> column, E value) {
        addCondition(column, LT, value);
        return this;
    }

    /**
     * {@code >=}
     *
     * @param column column
     * @param value  value
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> gte(MethodReference<T> column, E value) {
        addCondition(column, GTE, value);
        return this;
    }

    /**
     * {@code <=}
     *
     * @param column column
     * @param value  value
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> lte(MethodReference<T> column, E value) {
        addCondition(column, LTE, value);
        return this;
    }

    /**
     * {@code in (...)}
     *
     * @param column column
     * @param values {@link Collection} or Array.
     * @param <E>    value item type
     * @param <V>    value type
     * @return where builder
     */
    public <E, V extends Collection<E>> Where<T> in(MethodReference<T> column, V values) {
        criteria.add(new InCondition<>(getColumnName(column), IN, values));
        return this;
    }

    /**
     * {@code not in (...)}
     *
     * @param column column
     * @param values {@link Collection} or Array.
     * @param <E>    value item type
     * @param <V>    value type
     * @return where builder
     */
    public <E, V extends Collection<E>> Where<T> notIn(MethodReference<T> column, V values) {
        criteria.add(new InCondition<>(getColumnName(column), NOT_IN, values));
        return this;
    }

    /**
     * {@code between} a {@code and} b
     *
     * @param column column
     * @param a      value 1.
     * @param b      value 2.
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> between(MethodReference<T> column, E a, E b) {
        criteria.add(new BetweenCondition<>(getColumnName(column), BETWEEN, Pair.of(a, b)));
        return this;
    }

    /**
     * {@code not between} a {@code and} b
     *
     * @param column column
     * @param a      value 1.
     * @param b      value 2.
     * @param <E>    value type
     * @return where builder
     */
    public <E> Where<T> notBetween(MethodReference<T> column, E a, E b) {
        criteria.add(new BetweenCondition<>(getColumnName(column), NOT_BETWEEN, Pair.of(a, b)));
        return this;
    }

    /**
     * {@code like '%} str {@code %'}
     *
     * @param column column
     * @param value  value
     * @return where builder
     */
    public Where<T> like(MethodReference<T> column, String value) {
        addCondition(column, LIKE, "%" + value + "%");
        return this;
    }

    /**
     * {@code not like '%} str {@code %'}
     *
     * @param column column
     * @param value  value
     * @return where builder
     */
    public Where<T> notLike(MethodReference<T> column, String value) {
        addCondition(column, NOT_LIKE, "%" + value + "%");
        return this;
    }

    /**
     * {@code like '} str {@code %'}
     *
     * @param column column
     * @param value  value
     * @return where builder
     */
    public Where<T> startsWith(MethodReference<T> column, String value) {
        addCondition(column, LIKE, value + "%");
        return this;
    }

    /**
     * {@code not like '} str {@code %'}
     *
     * @param column column
     * @param value  value
     * @return where builder
     */
    public Where<T> notStartsWith(MethodReference<T> column, String value) {
        addCondition(column, NOT_LIKE, value + "%");
        return this;
    }

    /**
     * {@code like '%} str {@code '}
     *
     * @param column column
     * @param value  value
     * @return where builder
     */
    public Where<T> endsWith(MethodReference<T> column, String value) {
        addCondition(column, LIKE, "%" + value);
        return this;
    }

    /**
     * {@code not like '%} str {@code '}
     *
     * @param column column
     * @param value  value
     * @return where builder
     */
    public Where<T> notEndsWith(MethodReference<T> column, String value) {
        addCondition(column, NOT_LIKE, "%" + value);
        return this;
    }

    /**
     * {@code is null}
     *
     * @param column column
     * @return where builder
     */
    public Where<T> isNull(MethodReference<T> column) {
        addCondition(column, IS_NULL, null);
        return this;
    }

    /**
     * {@code is not null}
     *
     * @param column column
     * @return where builder
     */
    public Where<T> isNotNull(MethodReference<T> column) {
        addCondition(column, IS_NOT_NULL, null);
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
     * {@code (age &lt; 15 or age &gt; 60) and name = 'cyx'}
     * </pre></blockquote>
     * <blockquote><pre>
     * w -&gt; w.and(o -&gt; o.lt(Guest::getAge, 15)
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

    private void addCondition(MethodReference<T> column, Operator operator, Object value) {
        criteria.add(new Condition<>(getColumnName(column), operator, value));
    }

    /**
     * Returns where clause and params.
     *
     * @return where clause, params, identity columns.
     */
    protected @NotNull @Unmodifiable Pair<String, Map<String, Object>> build() {
        Pair<String, Map<String, Object>> where = build(new AtomicInteger(0), criteria, Logic.AND, 0);
        if (!where.getItem1().isEmpty()) {
            return Pair.of("\nwhere " + where.getItem1(), Collections.unmodifiableMap(where.getItem2()));
        }
        return where;
    }

    protected @NotNull Pair<String, Map<String, Object>> build(AtomicInteger uniqueIndex, List<Criteria> criteriaList, Logic logicOperator, int identLevel) {
        StringBuilder sb = new StringBuilder();
        String ident = StringUtil.repeat("    ", identLevel);
        Map<String, Object> params = new HashMap<>();
        for (int i = 0, j = criteriaList.size(); i < j; i++) {
            Criteria criteria = criteriaList.get(i);
            if (criteria instanceof Condition) {
                @SuppressWarnings("unchecked") Condition<Object> condition = (Condition<Object>) criteria;

                int unique = uniqueIndex.getAndIncrement();

                if (condition instanceof InCondition) {
                    @SuppressWarnings({"rawtypes", "unchecked"}) Pair<String, Map<String, Object>> result = ((InCondition) condition).buildStatement(unique, namedParamPrefix());
                    sb.append(result.getItem1());
                    params.putAll(result.getItem2());
                } else if (condition instanceof BetweenCondition) {
                    @SuppressWarnings({"rawtypes", "unchecked"}) Pair<String, Map<String, Object>> result = ((BetweenCondition) condition).buildStatement(unique, namedParamPrefix());
                    sb.append(result.getItem1());
                    params.putAll(result.getItem2());
                } else {
                    if (condition.getOperator() == IS_NULL || condition.getOperator() == IS_NOT_NULL) {
                        sb.append(condition.getColumn())
                                .append(" ")
                                .append(condition.getOperator().getValue());
                    } else {
                        String key = condition.getKey(unique);
                        sb.append(condition.getColumn())
                                .append(condition.getOperator().padWithSpace())
                                .append(namedParamPrefix()).append(key);
                        params.put(key, condition.getValue());
                    }
                }
            } else if (criteria instanceof AndGroup) {
                List<Criteria> andGroup = ((AndGroup) criteria).getGroup();
                if (!andGroup.isEmpty()) {
                    int groupSize = andGroup.size();
                    Pair<String, Map<String, Object>> result = build(uniqueIndex, andGroup, Logic.OR, groupSize == 1 ? identLevel : identLevel + 1);
                    if (groupSize == 1) {
                        sb.append(result.getItem1());
                    } else {
                        sb.append("(").append(result.getItem1()).append(")");
                    }
                    params.putAll(result.getItem2());
                }
            } else if (criteria instanceof OrGroup) {
                List<Criteria> orGroup = ((OrGroup) criteria).getGroup();
                if (!orGroup.isEmpty()) {
                    int groupSize = orGroup.size();
                    Pair<String, Map<String, Object>> result = build(uniqueIndex, orGroup, Logic.AND, groupSize == 1 ? identLevel : identLevel + 1);
                    if (groupSize == 1) {
                        sb.append(result.getItem1());
                    } else {
                        sb.append("(").append(result.getItem1()).append(")");
                    }
                    params.putAll(result.getItem2());
                }
            }

            if (i < j - 1) {
                sb.append("\n").append(ident);
                Criteria next = criteriaList.get(i + 1);
                // set the current logical operator where the next 'and' and 'or' group invoked
                if (next instanceof AndGroup) {
                    sb.append(Logic.AND.padWithSpace());
                } else if (next instanceof OrGroup) {
                    sb.append(Logic.OR.padWithSpace());
                } else {
                    sb.append(logicOperator.padWithSpace());
                }
            }
        }
        return Pair.of(sb.toString(), params);
    }
}