package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.sql.exceptions.DataAccessException;
import com.github.chengyuxing.sql.util.SqlGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public abstract class JdbcHelper {
    /**
     * Parse sql to executable prepared sql.
     *
     * @param sql  sql
     * @param args args
     * @return generated sql meta data
     */
    protected abstract SqlGenerator.PreparedSqlMetaData prepareSql(@NotNull String sql, Map<String, ?> args);

    /**
     * Handle prepared statement value.
     *
     * @param ps    PreparedStatement
     * @param index parameter index
     * @param value parameter value
     * @throws SQLException ex
     */
    protected abstract void doHandleStatementValue(@NotNull PreparedStatement ps,
                                                   @Range(from = 1, to = Integer.MAX_VALUE) int index,
                                                   @Nullable Object value) throws SQLException;

    /**
     * On statement created and do some configure before execute.
     *
     * @param statement statement
     * @param sql       sql
     * @param args      args
     * @throws SQLException if connection states error
     */
    protected abstract void onStatementInit(Statement statement, String sql, Map<String, ?> args) throws SQLException;

    /**
     * Set prepared sql statement args.
     *
     * @param ps    SQL statement object
     * @param args  args
     * @param names ordered arg names
     * @throws SQLException if connection states error
     */
    protected void setPreparedSqlArgs(PreparedStatement ps, Map<String, ?> args, Map<String, List<Integer>> names) throws SQLException {
        for (Map.Entry<String, List<Integer>> e : names.entrySet()) {
            String name = e.getKey();
            Object value = ValueUtils.getDeepValue(args, name);
            for (Integer i : e.getValue()) {
                doHandleStatementValue(ps, i, value);
            }
        }
    }

    /**
     * Wraps a given throwable into a {@link DataAccessException} with an optional SQL statement.
     *
     * @param sql       the SQL statement that caused the exception, may be null
     * @param throwable the original exception to wrap
     * @return a new {@link DataAccessException} that wraps the provided throwable and optionally includes the SQL statement
     */
    protected @NotNull RuntimeException wrappedDataAccessException(@Nullable String sql, @NotNull Throwable throwable) {
        if (throwable instanceof DataAccessException) {
            return (DataAccessException) throwable;
        }
        return sql == null
                ? new DataAccessException("Data access failed.", throwable)
                : new DataAccessException("Statement: " + sql, throwable);
    }
}
