package com.github.chengyuxing.sql.dsl.clause.condition;

import com.github.chengyuxing.sql.dsl.types.Operator;

public class Condition<T> implements Criteria {
    protected final String column;
    protected final Operator operator;
    protected final T value;
    protected final String valueKey;

    public Condition(String column, Operator operator, T value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.valueKey = column;
    }

    public Condition(String column, Operator operator, T value, String valueKey) {
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.valueKey = valueKey;
    }

    public String getColumn() {
        return column;
    }

    public Operator getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }

    public String getKey(int index) {
        return valueKey + "_" + index;
    }

    @Override
    public String toString() {
        return "Condition{" +
                "column='" + column + '\'' +
                ", operator=" + operator +
                ", value=" + value +
                '}';
    }
}