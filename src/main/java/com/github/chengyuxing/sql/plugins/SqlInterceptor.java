package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

import java.sql.DatabaseMetaData;
import java.util.Map;

@FunctionalInterface
public interface SqlInterceptor {
    /**
     * Pre-handle before SQL real executing, throw exception to reject SQL execution.
     *
     * @param rawSql    source sql: sql reference or sql content
     * @param parsedSql the parsed sql content
     * @param args      args
     * @param metaData  current database metadata
     * @return new parsed sql content
     */
    @NotNull String preHandle(@NotNull final String rawSql, @NotNull final String parsedSql, @NotNull Map<String, Object> args, @NotNull DatabaseMetaData metaData);
}
