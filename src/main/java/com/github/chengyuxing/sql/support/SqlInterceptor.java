package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.sql.exceptions.IllegalSqlException;

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
    boolean preHandle(String sql, Map<String, ?> args, DatabaseMetaData metaData) throws IllegalSqlException;
}
