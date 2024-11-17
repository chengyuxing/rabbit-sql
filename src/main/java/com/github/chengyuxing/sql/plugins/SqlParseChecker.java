package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

@FunctionalInterface
public interface SqlParseChecker {
    /**
     * Do something for sql.
     *
     * @param sql  sql
     * @param args args
     * @return new sql
     */
    @NotNull String handle(@NotNull final String sql, @NotNull Map<String, Object> args);
}
