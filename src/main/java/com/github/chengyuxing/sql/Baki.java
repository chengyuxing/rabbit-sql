package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.support.executor.EntityExecutor;
import com.github.chengyuxing.sql.support.executor.GenericExecutor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
     * Entity executor.
     * <p>Basic single entity CRUD depends on JPA.<br>
     * Implemented annotations: {@link javax.persistence.Entity @Entity}
     * {@link javax.persistence.Table @Table}
     * {@link javax.persistence.Id @Id}
     * {@link javax.persistence.Column @Column}
     * {@link javax.persistence.Transient @Transient}
     * </p>
     *
     * @param clazz entity class
     * @param <T>   entity type
     * @return Entity executor
     */
    @ApiStatus.AvailableSince("8.1.6")
    <T> EntityExecutor<T> entity(@NotNull Class<T> clazz);

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
    GenericExecutor of(@NotNull String sql);

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
    @NotNull DatabaseMetaData metaData();

    /**
     * Get current database name.
     *
     * @return database name
     */
    @NotNull String databaseId();
}
