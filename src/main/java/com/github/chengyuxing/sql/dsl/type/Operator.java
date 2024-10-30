package com.github.chengyuxing.sql.dsl.type;

@FunctionalInterface
public interface Operator {
    String getValue();

    default String padWithSpace() {
        return " " + getValue() + " ";
    }
}
