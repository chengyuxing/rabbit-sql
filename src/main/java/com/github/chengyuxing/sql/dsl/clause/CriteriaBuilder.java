package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.types.Logic;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.chengyuxing.sql.dsl.types.StandardOperator.IS_NOT_NULL;
import static com.github.chengyuxing.sql.dsl.types.StandardOperator.IS_NULL;

public abstract class CriteriaBuilder<T> extends ColumnHelper<T> {
    protected static final Object IDENTITY = new Object();

    protected CriteriaBuilder(@NotNull Class<T> clazz) {
        super(clazz);
    }

    protected abstract char namedParamPrefix();

    protected void doCheck(Condition<Object> condition) {
        if (isIllegalColumn(condition.getColumn())) {
            throw new IllegalArgumentException("unexpected column: '" + condition.getColumn() + "' on condition: " + condition);
        }
        if (isIllegalOperator(condition.getOperator())) {
            throw new IllegalArgumentException("Illegal operator: '" + condition.getOperator().getValue() + "' at condition: " + condition + ", add trusted operator into operatorWhiteList");
        }
    }

    protected final @NotNull Triple<String, Map<String, Object>, Set<String>> build(AtomicInteger uniqueIndex, List<Criteria> criteriaList, Logic logicOperator, int identLevel) {
        StringBuilder sb = new StringBuilder();
        String ident = StringUtil.repeat("    ", identLevel);
        Map<String, Object> params = new HashMap<>();
        Set<String> identity = new HashSet<>();
        for (int i = 0, j = criteriaList.size(); i < j; i++) {
            Criteria criteria = criteriaList.get(i);
            if (criteria instanceof Condition) {
                @SuppressWarnings("unchecked") Condition<Object> condition = (Condition<Object>) criteria;

                doCheck(condition);

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
                        boolean isIdentity = condition.getValue() == IDENTITY;
                        String key = condition.getKey(unique);
                        if (isIdentity) {
                            key = condition.getColumn();
                            identity.add(key);
                        }
                        sb.append(condition.getColumn())
                                .append(condition.getOperator().padWithSpace())
                                .append(namedParamPrefix()).append(key);
                        if (!isIdentity) {
                            params.put(key, condition.getValue());
                        }
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
        return Triple.of(sb.toString(), params, identity);
    }
}
