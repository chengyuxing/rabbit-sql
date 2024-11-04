package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.exceptions.IllegalSqlException;
import org.jetbrains.annotations.NotNull;

import java.sql.DatabaseMetaData;
import java.util.Map;

/**
 * Sql interceptor.
 */
@FunctionalInterface
public interface SqlInterceptor {
    /**
     * Pre handle before sql real execute.
     *
     * @param sql      sql
     * @param args     sql parameter data
     * @param metaData current database metadata
     * @return true if valid or false
     * @throws IllegalSqlException reject execute exception
     */
    boolean preHandle(@NotNull String sql, @NotNull Map<String, ?> args, @NotNull DatabaseMetaData metaData) throws IllegalSqlException;
}
