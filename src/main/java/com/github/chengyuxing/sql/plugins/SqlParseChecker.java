package com.github.chengyuxing.sql.plugins;

@FunctionalInterface
public interface SqlParseChecker {
    /**
     * Do something for sql.
     *
     * @param sql sql
     * @return new sql
     */
    String handle(final String sql);
}
