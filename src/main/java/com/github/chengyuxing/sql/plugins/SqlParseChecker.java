package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface SqlParseChecker {
    /**
     * Do something for sql.
     *
     * @param sql sql
     * @return new sql
     */
    @NotNull String handle(@NotNull final String sql);
}
