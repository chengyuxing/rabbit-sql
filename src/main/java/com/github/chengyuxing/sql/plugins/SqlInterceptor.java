package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.types.DatabaseInfo;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Sql interceptor.
 */
@FunctionalInterface
public interface SqlInterceptor {
    /**
     * Pre-handle before SQL real executing, throw exception to reject SQL execution.
     *
     * @param rawSql    source SQL: SQL reference or SQL content
     * @param parsedSql the parsed SQL content
     * @param args      args
     * @param info      current database information
     * @return new parsed SQL content
     */
    @NotNull String preHandle(@NotNull final String rawSql, @NotNull final String parsedSql, @NotNull Map<String, ?> args, @NotNull DatabaseInfo info);
}
