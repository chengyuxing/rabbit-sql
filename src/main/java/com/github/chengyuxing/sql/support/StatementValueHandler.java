package com.github.chengyuxing.sql.support;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Prepared statement parameter value handler.
 */
@FunctionalInterface
public interface StatementValueHandler {
    /**
     * Custom handle statement parameter value.<br>
     * Do some special thing for parameter value by {@link DatabaseMetaData},  e.g.
     * <ul>
     *     <li>{@link DatabaseMetaData#getDatabaseProductName() getDatabaseProductName()} get current database name;</li>
     *     <li>{@link DatabaseMetaData#getDatabaseProductVersion() getDatabaseProductVersion()} get current database version.</li>
     * </ul>
     *
     * @param ps       prepared statement object ({@link PreparedStatement} | {@link java.sql.CallableStatement CallableStatement})
     * @param index    parameter index
     * @param value    parameter value
     * @param metaData current database metadata
     * @throws SQLException if connection states error
     * @see com.github.chengyuxing.sql.utils.JdbcUtil#setStatementValue(PreparedStatement, int, Object) JdbcUtil.setStatementValue(PreparedStatement, int, Object)
     */
    void preHandle(PreparedStatement ps, int index, Object value, DatabaseMetaData metaData) throws SQLException;
}
