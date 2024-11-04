package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

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
    @NotNull String format(Object value);
}
