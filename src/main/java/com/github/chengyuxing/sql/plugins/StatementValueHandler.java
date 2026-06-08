package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.types.DatabaseInfo;
import com.github.chengyuxing.sql.util.JdbcUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Prepared statement parameter value handler.
 */
@FunctionalInterface
public interface StatementValueHandler {
    /**
     * Custom handle statement parameter value.<br>
     * For support database features do some converts for parameter value by check {@link DatabaseInfo},  e.g.
     * <ul>
     *     <li>{@link DatabaseInfo#getName() getName()} get current database name;</li>
     *     <li>{@link DatabaseInfo#getVersion() getVersion()} get current database version.</li>
     * </ul>
     *
     * @param ps    prepared statement object ({@link PreparedStatement} | {@link java.sql.CallableStatement CallableStatement})
     * @param index parameter index
     * @param value parameter value
     * @param info  current database information
     * @throws SQLException if connection states error
     * @see JdbcUtils#setStatementValue(PreparedStatement, int, Object) JdbcUtil.setStatementValue(PreparedStatement, int, Object)
     */
    void handle(@NotNull PreparedStatement ps,
                @Range(from = 1, to = Integer.MAX_VALUE) int index,
                @Nullable Object value,
                @NotNull DatabaseInfo info) throws SQLException;
}
