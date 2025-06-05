package com.github.chengyuxing.sql.dsl.clause.condition;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.types.Logic;
import com.github.chengyuxing.sql.dsl.types.StandardOperator;

import java.util.HashMap;
import java.util.Map;

public class BetweenCondition<T> extends Condition<Pair<T, T>> {
    public BetweenCondition(String column, StandardOperator operator, Pair<T, T> value) {
        super(column, operator, value);
    }

    public BetweenCondition(String column, StandardOperator operator, Pair<T, T> value, String valueKey) {
        super(column, operator, value, valueKey);
    }

    public Pair<String, Map<String, Object>> buildStatement(int index, char namedParamPrefix) {
        String a = valueKey + "_" + index + "_0";
        String b = valueKey + "_" + index + "_1";
        String statement = column + operator.padWithSpace() + namedParamPrefix + a + Logic.AND.padWithSpace() + namedParamPrefix + b;
        Map<String, Object> params = new HashMap<>();
        params.put(a, value.getItem1());
        params.put(b, value.getItem2());
        return Pair.of(statement, params);
    }
}
