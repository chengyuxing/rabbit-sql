package com.github.chengyuxing.sql.support;

@FunctionalInterface
public interface NamedParamFormatter {
    /**
     * Non-prepared Sql named parameter ({@code :key}) value formatter.
     *
     * @param value value
     */
    String format(Object value);
}
