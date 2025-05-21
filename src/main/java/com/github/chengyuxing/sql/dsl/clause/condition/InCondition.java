package com.github.chengyuxing.sql.dsl.clause.condition;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.dsl.types.StandardOperator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class InCondition<T> extends Condition<Collection<T>> {
    public InCondition(String column, StandardOperator operator, Collection<T> value) {
        super(column, operator, value);
    }

    public InCondition(String column, StandardOperator operator, Collection<T> value, String valueKey) {
        super(column, operator, value, valueKey);
    }

    public Pair<String, Map<String, Object>> buildStatement(int index, char namedParamPrefix) {
        StringJoiner sb = new StringJoiner(", ", "(", ")");
        Map<String, Object> params = new HashMap<>();
        Object[] values = ObjectUtil.toArray(value);
        for (int i = 0; i < values.length; i++) {
            String key = valueKey + "_" + index + "_" + i;
            sb.add(namedParamPrefix + key);
            params.put(key, values[i]);
        }
        return Pair.of(column + operator.padWithSpace() + sb, params);
    }
}
