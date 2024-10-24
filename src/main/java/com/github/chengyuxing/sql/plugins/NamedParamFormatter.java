package com.github.chengyuxing.sql.plugins;

/**
 * Non-prepared Sql named parameter ({@code :key}) value formatter.
 */
@FunctionalInterface
public interface NamedParamFormatter {
    /**
     * Format named parameter to string literal.
     *
     * @param value value
     * @return formatted value
     */
    String format(Object value);
}
