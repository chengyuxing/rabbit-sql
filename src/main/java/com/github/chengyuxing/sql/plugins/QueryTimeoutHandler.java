package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.Range;

import java.sql.Statement;
import java.util.Map;

/**
 * Jdbc execute sql timeout({@link Statement#setQueryTimeout(int)}) handler.
 */
@FunctionalInterface
public interface QueryTimeoutHandler {
    /**
     * Do handle sql execute timeout.
     *
     * @param sql  sql
     * @param args args
     * @return timeout (seconds)
     */
    @Range(from = 0, to = Integer.MAX_VALUE)
    int handle(String sql, Map<String, ?> args);
}
