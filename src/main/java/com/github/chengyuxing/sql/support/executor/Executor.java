package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.types.Param;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Basic executor.
 */
public interface Executor {
    /**
     * Execute sql (ddl, dml, query or plsql).
     *
     * @return Query: List{@code <DataRow>}, DML: affected row count, DDL: 0
     * @see com.github.chengyuxing.sql.support.JdbcSupport#execute(String, Map) execute(String, Map)
     */
    @NotNull DataRow execute();

    /**
     * Execute sql (ddl, dml, query or plsql).
     *
     * @param args args
     * @return Query: List{@code <DataRow>}, DML: affected row count, DDL: 0
     * @see com.github.chengyuxing.sql.support.JdbcSupport#execute(String, Map) execute(String, Map)
     */
    @NotNull DataRow execute(Map<String, ?> args);

    /**
     * Batch execute non-prepared sql (dml, ddl).
     *
     * @param moreSql more sql
     * @return affected row count
     */
    int executeBatch(String... moreSql);

    /**
     * Batch execute non-prepared sql (dml, ddl).
     *
     * @param moreSql more sql
     * @return affected row count
     */
    int executeBatch(@NotNull List<String> moreSql);

    /**
     * Batch execute prepared dml sql.
     *
     * @param args args collection
     * @return affected row count
     */
    int executeBatch(@NotNull Collection<? extends Map<String, ?>> args);

    /**
     * Execute store procedure or function.
     *
     * @param params in,out and in_out params
     * @return DataRow
     * @see com.github.chengyuxing.sql.support.JdbcSupport#executeCallStatement(String, Map) executeCallStatement(String, Map)
     */
    @NotNull DataRow call(Map<String, Param> params);
}
