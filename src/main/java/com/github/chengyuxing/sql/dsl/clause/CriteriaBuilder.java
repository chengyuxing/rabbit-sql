package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.dsl.clause.condition.*;
import com.github.chengyuxing.sql.dsl.type.Logic;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.chengyuxing.sql.dsl.type.StandardOperator.IS_NOT_NULL;
import static com.github.chengyuxing.sql.dsl.type.StandardOperator.IS_NULL;

public abstract class CriteriaBuilder<T> extends ColumnHelper<T> {
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

    protected final @NotNull Pair<String, Map<String, Object>> build(AtomicInteger uniqueIndex, List<Criteria> criteriaList, Logic logicOperator, int identLevel) {
        StringBuilder sb = new StringBuilder();
        String ident = StringUtil.repeat("    ", identLevel);
        Map<String, Object> params = new HashMap<>();
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
                    Pair<String, Map<String, Object>> result = ((BetweenCondition) condition).buildStatement(unique, namedParamPrefix());
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
                    Pair<String, Map<String, Object>> result = build(uniqueIndex, andGroup, Logic.AND, identLevel + 1);
                    sb.append("(").append(result.getItem1()).append(")");
                    params.putAll(result.getItem2());
                }
            } else if (criteria instanceof OrGroup) {
                List<Criteria> orGroup = ((OrGroup) criteria).getGroup();
                if (!orGroup.isEmpty()) {
                    Pair<String, Map<String, Object>> result = build(uniqueIndex, orGroup, Logic.OR, identLevel + 1);
                    sb.append("(").append(result.getItem1()).append(")");
                    params.putAll(result.getItem2());
                }
            }

            if (i < j - 1) {
                sb.append("\n").append(ident);
                Criteria next = criteriaList.get(i + 1);
                // set current logical operator where the next 'and' and 'or' group invoked
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
