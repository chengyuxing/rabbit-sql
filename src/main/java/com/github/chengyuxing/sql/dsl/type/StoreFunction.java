package com.github.chengyuxing.sql.dsl.type;

@FunctionalInterface
public interface StoreFunction {
    String apply(String column);
}
