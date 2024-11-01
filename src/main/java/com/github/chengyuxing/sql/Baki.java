package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.dsl.*;
import com.github.chengyuxing.sql.support.executor.Executor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.function.Function;

/**
 * Basic database access interface
 * work perfectly with <a href="https://plugins.jetbrains.com/plugin/21403-rabbit-sql">Rabbit-SQL IDEA Plugin</a>.
 */
public interface Baki {
    /**
     * Query executor.
     *
     * @param sql sql statement or sql name
     * @return Query executor
     */
    QueryExecutor query(@NotNull String sql);

    /**
     * Query.
     *
     * @param clazz entity class
     * @param <T>   entity type
     * @return Query instance
     * @see javax.persistence.Entity @Entity
     */
    <T, SELF extends Query<T, SELF>> Query<T, SELF> query(@NotNull Class<T> clazz);

    /**
     * Insert.
     *
     * @param entity entity
     * @param <T>    entity type
     * @return affected rows
     * @see javax.persistence.Entity @Entity
     */
    <T> int insert(@NotNull T entity);

    /**
     * Batch insert.
     *
     * @param entities entities
     * @param <T>      entity type
     * @return affected rows
     * @see javax.persistence.Entity @Entity
     */
    <T> int insert(@NotNull Collection<T> entities);

    /**
     * Update.
     *
     * @param entity entity
     * @param <T>    entity type
     * @return Update instance
     * @see javax.persistence.Entity @Entity
     */
    <T> Update<T> update(@NotNull T entity);

    /**
     * Delete.
     *
     * @param entity entity
     * @param <T>    entity type
     * @return Delete instance
     * @see javax.persistence.Entity @Entity
     */
    <T> Delete<T> delete(@NotNull T entity);

    /**
     * Basic executor.
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
    Executor of(@NotNull String sql);

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
     * Offline(which connection was closed) database metadata, maybe proxy databaseMetadata of
     * some datasource has different implementation.
     *
     * @return current database metadata
     */
    DatabaseMetaData metaData();

    /**
     * Get current database name.
     *
     * @return database name
     */
    @NotNull String databaseId();
}
