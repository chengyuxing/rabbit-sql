package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.support.executor.Executor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.SaveExecutor;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.function.Function;

/**
 * Basic database access interface.
 */
public interface Baki {
    /**
     * Query executor.
     *
     * @param sql sql statement or sql name
     * @return Query executor
     */
    QueryExecutor query(String sql);

    /**
     * Update executor.<br>
     * About example:
     * <blockquote>
     * <pre>
     *  args： {id:14, name:'cyx', address:'kunming'},{...}...
     *  where condition："id = :id"
     *  generate：update{@code <table>} set name = :name, address = :address
     *       where id = :id
     *  </pre>
     * Notice: where condition must contains at least 1 named parameter and args must contains it's value.
     * </blockquote>
     *
     * @param tableName table name
     * @param where     condition
     * @return Update executor
     */
    SaveExecutor update(String tableName, String where);

    /**
     * Insert executor.
     *
     * @param tableName table name
     * @return Insert executor
     */
    SaveExecutor insert(String tableName);

    /**
     * Delete executor.<br>
     * Methods {@link SaveExecutor#safe() safe(boolean?)} and {@link SaveExecutor#ignoreNull() ignoreNull(boolean?)}
     * were not implements, ignore please.
     *
     * @param tableName table name
     * @param where     condition
     * @return Delete executor
     */
    SaveExecutor delete(String tableName, String where);

    /**
     * Basic Executor.
     *
     * @param sql Support：<ul>
     *            <li>ddl</li>
     *            <li>dml</li>
     *            <li>query</li>
     *            <li>function/procedure</li>
     *            <li>plsql</li>
     *            </ul>
     * @return Basic executor
     */
    Executor of(String sql);

    /**
     * Get an auto-closeable connection.
     *
     * @param func connection provide callback
     * @param <T>  result type
     * @return any result
     */
    <T> T using(Function<Connection, T> func);

    /**
     * Get current database metadata.<br>
     * Offline(which connection was closed) database metadata, maybe proxy databaseMetadata of
     * some datasource has different implementation.
     *
     * @return current database metadata
     */
    DatabaseMetaData metaData();
}
