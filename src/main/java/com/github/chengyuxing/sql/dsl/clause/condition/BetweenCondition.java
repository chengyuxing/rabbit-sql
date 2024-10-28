package com.github.chengyuxing.sql.dsl.clause.condition;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.type.StandardOperator;

import java.util.HashMap;
import java.util.Map;

public class BetweenCondition extends Condition<Object> {
    public BetweenCondition(String column, StandardOperator operator, Pair<Object, Object> value) {
        super(column, operator, value);
    }

    public Pair<String, Map<String, Object>> buildStatement(int index, char namedParamPrefix) {
        //noinspection unchecked
        Pair<Object, Object> pair = (Pair<Object, Object>) value;
        String a = column + "__" + index + "_0";
        String b = column + "__" + index + "_1";
        String statement = column + operator.getValue() + namedParamPrefix + a + StandardOperator.LOGIC_AND + namedParamPrefix + b;
        Map<String, Object> params = new HashMap<>();
        params.put(a, pair.getItem1());
        params.put(b, pair.getItem2());
        return Pair.of(statement, params);
    }
}
