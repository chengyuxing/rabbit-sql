package com.github.chengyuxing.sql.dsl.types;

public enum StandardAggFunction implements StoreFunction {
    COUNT("count"),
    SUM("sum"),
    MIN("min"),
    MAX("max"),
    AVG("avg");
    private final String name;

    StandardAggFunction(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String apply(String column) {
        return name + "(" + column + ")";
    }
}
