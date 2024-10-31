package com.github.chengyuxing.sql.dsl.type;

public enum Logic implements Operator {
    AND("and"),
    OR("or");
    private final String value;

    Logic(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }
}
