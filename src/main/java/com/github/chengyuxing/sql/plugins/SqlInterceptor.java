package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

import java.sql.DatabaseMetaData;
import java.util.Map;

/**
 * Sql interceptor.
 */
@FunctionalInterface
public interface SqlInterceptor {
    /**
     * Pre-handle before SQL real executing, throw exception to reject SQL execution.
     *
     * @param sql      sql
     * @param args     sql parameter data
     * @param metaData current database metadata
     */
    void preHandle(@NotNull String sql, @NotNull Map<String, ?> args, @NotNull DatabaseMetaData metaData);
}
