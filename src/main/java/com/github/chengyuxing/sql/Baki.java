package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.dsl.*;
import com.github.chengyuxing.sql.support.executor.Executor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.function.Function;

/**
 * Basic database access interface
 * work perfectly with <a href="https://plugins.jetbrains.com/plugin/21403-rabbit-sql">Rabbit-SQL IDEA Plugin</a>,
 * Features:
 * <ul>
 *     <li>Native sql execute.</li>
 *     <li>Basic single entity CRUD depends on JPA.<br>
 *     Implemented annotations: {@link jakarta.persistence.Entity @Entity}
 *     {@link jakarta.persistence.Table @Table}
 *     {@link jakarta.persistence.Id @Id}
 *     {@link jakarta.persistence.Column @Column}
 *     {@link jakarta.persistence.Transient @Transient}
 *     </li>
 * </ul>
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
     * @param clazz  entity class
     * @param <T>    entity type
     * @param <SELF> query builder
     * @return Query instance
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.0")
    <T, SELF extends Query<T, SELF>> Query<T, SELF> query(@NotNull Class<T> clazz);

    /**
     * Insert.
     *
     * @param entity entity
     * @param <T>    entity type
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.0")
    <T> int insert(@NotNull T entity);

    /**
     * Batch insert.
     *
     * @param entities entities
     * @param <T>      entity type
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.0")
    <T> int insert(@NotNull Collection<T> entities);

    /**
     * Update by {@link jakarta.persistence.Id id}.
     *
     * @param entity     entity
     * @param ignoreNull ignore null value or not for update sets
     * @param <T>        entity type
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    <T> int update(@NotNull T entity, boolean ignoreNull);

    /**
     * Batch update by {@link jakarta.persistence.Id id}.
     * <p>Notice: the real update statement depends on first data,
     * it means 'ignoreNull' just available on first data.</p>
     *
     * @param entities   entities
     * @param ignoreNull ignore null value or not for update sets
     * @param <T>        entity type
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    <T> int update(@NotNull Collection<T> entities, boolean ignoreNull);

    /**
     * Delete by {@link jakarta.persistence.Id id}.
     *
     * @param entity entity
     * @param <T>    entity type
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    <T> int delete(@NotNull T entity);

    /**
     * Batch delete by {@link jakarta.persistence.Id id}.
     *
     * @param entities entities
     * @param <T>      entity type
     * @return affected rows
     * @see jakarta.persistence.Entity @Entity
     */
    @ApiStatus.AvailableSince("8.0.1")
    <T> int delete(@NotNull Collection<T> entities);

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
