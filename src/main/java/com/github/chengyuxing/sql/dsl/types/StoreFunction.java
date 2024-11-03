package com.github.chengyuxing.sql.dsl.types;

@FunctionalInterface
public interface StoreFunction {
    String apply(String column);
}
