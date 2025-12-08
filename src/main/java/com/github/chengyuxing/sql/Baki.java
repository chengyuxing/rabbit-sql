package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.plugins.QueryExecutor;
import com.github.chengyuxing.sql.plugins.SimpleDMLExecutor;
import com.github.chengyuxing.sql.types.Param;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.function.Function;

/**
 * Basic database access interface
 * work perfectly with <a href="https://plugins.jetbrains.com/plugin/21403-rabbit-sql">Rabbit-SQL IDEA Plugin</a>.
 */
public interface Baki {
    /**
     * Query executor.
     *
     * @param sql sql statement
     * @return Query executor
     */
    QueryExecutor query(@NotNull String sql);

    /**
     * Execute insert.
     *
     * @param sql  sql
     * @param data data
     * @return affected row count
     */
    int insert(@NotNull String sql, @NotNull Map<String, Object> data);

    /**
     * Execute batch insert.
     *
     * @param sql  sql
     * @param data data
     * @return affected row count
     */
    int insert(@NotNull String sql, @NotNull Iterable<? extends Map<String, Object>> data);

    /**
     * Execute update.
     *
     * @param sql  sql
     * @param args args
     * @return affected row count
     */
    int update(@NotNull String sql, Map<String, Object> args);

    /**
     * Execute batch update.
     *
     * @param sql  sql
     * @param args args
     * @return affected row count
     */
    int update(@NotNull String sql, @NotNull Iterable<? extends Map<String, Object>> args);

    /**
     * Execute delete.
     *
     * @param sql  sql
     * @param args args
     * @return affected row count
     */
    int delete(@NotNull String sql, Map<String, Object> args);

    /**
     * Execute batch delete.
     *
     * @param sql  sql
     * @param args args
     * @return affected row count
     */
    int delete(@NotNull String sql, @NotNull Iterable<? extends Map<String, Object>> args);

    /**
     * Execute store procedure or function.
     *
     * @param procedure procedure or function statement
     * @param params    in,out and in_out params
     * @return DataRow
     */
    @NotNull DataRow call(@NotNull String procedure, Map<String, Param> params);

    /**
     * Execute sql (ddl, dml, query or plsql).
     *
     * @param sql  sql statement
     * @param args args
     * @return DataRow
     */
    @NotNull DataRow execute(@NotNull String sql, Map<String, Object> args);

    /**
     * Execute batch prepared dml sql.
     *
     * @param sql  sql statement
     * @param args args
     * @return affected row count
     */
    int execute(@NotNull String sql, @NotNull Iterable<? extends Map<String, Object>> args);

    /**
     * Batch execute non-prepared sql (dml, ddl).
     *
     * @param sqlList sql statements
     * @return affected row count
     */
    int execute(@NotNull Iterable<String> sqlList);

    /**
     * Get an auto-closeable connection.
     *
     * @param func connection -&gt; any
     * @param <T>  result type
     * @return any result
     */
    <T> T using(Function<Connection, T> func);

    /**
     * Get current database metadata.<br>
     * Offline (which connection was closed) database metadata, maybe proxy databaseMetadata of
     * some datasource has different implementation.
     *
     * @return current database metadata
     */
    @NotNull DatabaseMetaData metaData();

    /**
     * Get current database name.
     *
     * @return database name
     */
    @NotNull String databaseId();
}
