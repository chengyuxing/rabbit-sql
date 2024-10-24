package com.github.chengyuxing.sql.plugins;

@FunctionalInterface
public interface AfterParseDynamicSql {
    /**
     * Do something for sql.
     *
     * @param sql sql
     * @return new sql
     */
    String handle(final String sql);
}
