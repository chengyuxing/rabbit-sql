package com.github.chengyuxing.sql.dsl.types;

@FunctionalInterface
public interface Operator {
    String getValue();

    default String padWithSpace() {
        return " " + getValue() + " ";
    }
}
