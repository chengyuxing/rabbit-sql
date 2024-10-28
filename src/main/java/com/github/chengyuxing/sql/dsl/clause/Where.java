package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.type.ColumnReference;
import com.github.chengyuxing.sql.dsl.type.Operator;
import com.github.chengyuxing.sql.utils.EntityUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static com.github.chengyuxing.sql.dsl.type.StandardOperator.*;

public abstract class Where<T, SELF extends Where<T, SELF>> {
    protected List<Criteria> conditions = new ArrayList<>();
    protected final Class<T> entityClass;

    /**
     * Construct a new Where builder.
     */
    public Where(Class<T> entityClass) {
        this.entityClass = entityClass;
    }

    /**
     * Construct a new Where builder with initial where builder.
     *
     * @param where where builder
     */
    public Where(Class<T> entityClass, @NotNull Where<T, SELF> where) {
        this.entityClass = entityClass;
        if (this.entityClass != where.entityClass) {
            throw new IllegalArgumentException("Unexpected entity class: " + where.entityClass.getName() + ", expected: " + this.entityClass.getName());
        }
        this.conditions = where.conditions;
    }

    /**
     * Returns a default where builder.
     *
     * @param <SELF> self type
     * @return where builder
     */
    public static <T, SELF extends Where<T, SELF>> Where<T, SELF> of(@NotNull Class<T> entityClass) {
        return new Where<T, SELF>(entityClass) {
            @Override
            protected char namedParamPrefix() {
                throw new UnsupportedOperationException("Should not be called here");
            }

            @Override
            protected void checkConditionField(String field) {
                throw new UnsupportedOperationException("Should not be called here");
            }
        };
    }

    protected abstract char namedParamPrefix();

    protected abstract void checkConditionField(String field);

    @SafeVarargs
    public final <E> SELF of(@NotNull String column, @NotNull Operator operator, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, operator, value);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF of(ColumnReference<T> column, @NotNull Operator operator, E value, Predicate<E>... predicates) {
        return of(EntityUtil.getFieldNameWithCache(column), operator, value, predicates);
    }

    @SafeVarargs
    public final <E> SELF eq(@NotNull String column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            if (Objects.nonNull(value)) {
                addCondition(column, EQ, value);
            } else {
                addCondition(column, IS, null);
            }
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF eq(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        return eq(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final <E> SELF ne(@NotNull String column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            if (Objects.nonNull(value)) {
                addCondition(column, NEQ, value);
            } else {
                addCondition(column, IS_NOT, null);
            }
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF ne(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        return ne(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final <E> SELF gt(@NotNull String column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, GT, value);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF gt(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        return gt(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final <E> SELF lt(@NotNull String column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LT, value);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF lt(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        return lt(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final <E> SELF gte(@NotNull String column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, GTE, value);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF gte(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        return gte(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final <E> SELF lte(@NotNull String column, E value, Predicate<E>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LTE, value);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF lte(ColumnReference<T> column, E value, Predicate<E>... predicates) {
        return lte(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final <E, V extends Collection<E>> SELF in(@NotNull String column, V values, Predicate<V>... predicates) {
        if (isConditionMatched(values, predicates)) {
            conditions.add(new InCondition<>(column, IN, values));
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E, V extends Collection<E>> SELF in(ColumnReference<T> column, V values, Predicate<V>... predicates) {
        return in(EntityUtil.getFieldNameWithCache(column), values, predicates);
    }

    @SafeVarargs
    public final <E, V extends Collection<E>> SELF notIn(@NotNull String column, V values, Predicate<V>... predicates) {
        if (isConditionMatched(values, predicates)) {
            conditions.add(new InCondition<>(column, NOT_IN, values));
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E, V extends Collection<E>> SELF notIn(ColumnReference<T> column, V values, Predicate<V>... predicates) {
        return notIn(EntityUtil.getFieldNameWithCache(column), values, predicates);
    }

    @SafeVarargs
    public final <E> SELF between(@NotNull String column, E a, E b, BiPredicate<E, E>... predicates) {
        if (isConditionMatched(a, b, predicates)) {
            conditions.add(new BetweenCondition(column, BETWEEN, Pair.of(a, b)));
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF between(ColumnReference<T> column, E a, E b, BiPredicate<E, E>... predicates) {
        return between(EntityUtil.getFieldNameWithCache(column), a, b, predicates);
    }

    @SafeVarargs
    public final <E> SELF notBetween(@NotNull String column, E a, E b, BiPredicate<E, E>... predicates) {
        if (isConditionMatched(a, b, predicates)) {
            conditions.add(new BetweenCondition(column, NOT_BETWEEN, Pair.of(a, b)));
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final <E> SELF notBetween(ColumnReference<T> column, E a, E b, BiPredicate<E, E>... predicates) {
        return notBetween(EntityUtil.getFieldNameWithCache(column), a, b, predicates);
    }

    @SafeVarargs
    public final SELF like(@NotNull String column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, "%" + value + "%");
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final SELF like(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        return like(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final SELF notLike(@NotNull String column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, "%" + value + "%");
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final SELF notLike(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        return notLike(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final SELF startsWith(@NotNull String column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, value + "%");
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final SELF startsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        return startsWith(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final SELF notStartsWith(@NotNull String column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, value + "%");
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final SELF notStartsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        return notStartsWith(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final SELF endsWith(@NotNull String column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, LIKE, "%" + value);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final SELF endsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        return endsWith(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    @SafeVarargs
    public final SELF notEndsWith(@NotNull String column, @NotNull String value, Predicate<String>... predicates) {
        if (isConditionMatched(value, predicates)) {
            addCondition(column, NOT_LIKE, "%" + value);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    @SafeVarargs
    public final SELF notEndsWith(ColumnReference<T> column, @NotNull String value, Predicate<String>... predicates) {
        return notEndsWith(EntityUtil.getFieldNameWithCache(column), value, predicates);
    }

    public SELF isNull(@NotNull String column, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            addCondition(column, IS, null);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    public SELF isNull(ColumnReference<T> column, boolean... predicates) {
        return isNull(EntityUtil.getFieldNameWithCache(column), predicates);
    }

    public SELF isNotNull(@NotNull String column, boolean... predicates) {
        if (isConditionMatched(predicates)) {
            addCondition(column, IS_NOT, null);
        }
        //noinspection unchecked
        return (SELF) this;
    }

    public SELF isNotNull(ColumnReference<T> column, boolean... predicates) {
        return isNotNull(EntityUtil.getFieldNameWithCache(column), predicates);
    }

    public <ENTITY, NEW extends Where<ENTITY, NEW>> SELF and(@NotNull Where<ENTITY, NEW> ands) {
        if (this.entityClass != ands.entityClass) {
            throw new IllegalArgumentException("Unexpected entity class: " + ands.entityClass.getName() + ", expected: " + this.entityClass.getName());
        }
        conditions.add(new AndGroup(ands.conditions));
        //noinspection unchecked
        return (SELF) this;
    }

    public <ENTITY, NEW extends Where<ENTITY, NEW>> SELF or(@NotNull Where<ENTITY, NEW> ors) {
        if (this.entityClass != ors.entityClass) {
            throw new IllegalArgumentException("Unexpected entity class: " + ors.entityClass.getName() + ", expected: " + this.entityClass.getName());
        }
        conditions.add(new OrGroup(ors.conditions));
        //noinspection unchecked
        return (SELF) this;
    }

    /**
     * Returns a where condition consisting of the where builder, checking the building result currently.
     *
     * @param consumer built result consumer (where, params) -&gt; _
     * @return self
     */
    public SELF peek(BiConsumer<String, Map<String, Object>> consumer) {
        Pair<String, Map<String, Object>> where = buildWhereClause();
        consumer.accept(where.getItem1().trim(), where.getItem2());
        //noinspection unchecked
        return (SELF) this;
    }

    protected @NotNull @Unmodifiable Pair<String, Map<String, Object>> buildWhereClause() {
        Pair<String, Map<String, Object>> where = buildWhereClause(new AtomicInteger(0), conditions, LOGIC_AND.getValue());
        if (!where.getItem1().isEmpty()) {
            return Pair.of("\nwhere " + where.getItem1(), Collections.unmodifiableMap(where.getItem2()));
        }
        return where;
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

    private void addCondition(String field, Operator operator, Object value) {
        conditions.add(new Condition<>(field, operator, value));
    }

    Pair<String, Map<String, Object>> buildWhereClause(AtomicInteger uniqueIndex, List<Criteria> criteriaList, String concat) {
        StringJoiner where = new StringJoiner(concat);
        Map<String, Object> params = new HashMap<>();
        for (Criteria criteria : criteriaList) {
            if (criteria instanceof Condition) {
                int unique = uniqueIndex.getAndIncrement();
                @SuppressWarnings("unchecked") Condition<Object> condition = (Condition<Object>) criteria;

                checkConditionField(condition.getColumn());

                if (condition instanceof InCondition) {
                    @SuppressWarnings({"rawtypes", "unchecked"}) Pair<String, Map<String, Object>> result = ((InCondition) condition).buildStatement(unique, namedParamPrefix());
                    where.add(result.getItem1());
                    params.putAll(result.getItem2());
                    continue;
                }

                if (condition instanceof BetweenCondition) {
                    Pair<String, Map<String, Object>> result = ((BetweenCondition) condition).buildStatement(unique, namedParamPrefix());
                    where.add(result.getItem1());
                    params.putAll(result.getItem2());
                    continue;
                }

                String key = condition.getKey(unique);
                where.add(condition.getColumn() + condition.getOperator().getValue() + namedParamPrefix() + key);
                params.put(key, condition.getValue());
                continue;
            }

            if (criteria instanceof AndGroup) {
                List<Criteria> andGroup = ((AndGroup) criteria).getGroup();
                if (!andGroup.isEmpty()) {
                    Pair<String, Map<String, Object>> result = buildWhereClause(uniqueIndex, andGroup, LOGIC_AND.getValue());
                    where.add("(" + result.getItem1() + ")");
                    params.putAll(result.getItem2());
                }
                continue;
            }

            if (criteria instanceof OrGroup) {
                List<Criteria> orGroup = ((OrGroup) criteria).getGroup();
                if (!orGroup.isEmpty()) {
                    Pair<String, Map<String, Object>> result = buildWhereClause(uniqueIndex, orGroup, LOGIC_OR.getValue());
                    where.add("(" + result.getItem1() + ")");
                    params.putAll(result.getItem2());
                }
            }
        }
        return Pair.of(where.toString(), params);
    }
}
