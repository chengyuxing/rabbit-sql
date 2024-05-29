package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.support.executor.EntitySaveExecutor;
import com.github.chengyuxing.sql.support.executor.Executor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.SaveExecutor;

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
    QueryExecutor query(String sql);

    /**
     * Update executor.
     * <p>Generate update statement by 1st row of data, e.g.</p>
     * <p>args:</p>
     * <blockquote>
     * <pre>{id:14, name:'cyx', address:'kunming'}, {...}, ...
     *  </pre></blockquote>
     * <p>where condition：</p>
     * <blockquote>
     * <pre>id = :id</pre>
     * </blockquote>
     * <p>generated update statement：</p>
     * <blockquote>
     * <pre>update [table] set
     * name = :name,
     * address = :address
     * where id = :id</pre>
     * </blockquote>
     * Notice: where condition must contain at least 1 named parameter and args must contain its value.
     *
     * @param tableName table name
     * @param where     condition
     * @return Update executor
     */
    <T> SaveExecutor<T> update(String tableName, String where);

    /**
     * Insert executor.
     *
     * @param tableName table name
     * @return Insert executor
     */
    <T> SaveExecutor<T> insert(String tableName);

    /**
     * Delete executor.
     * <p>Methods {@link SaveExecutor#safe() safe(boolean?)} and {@link SaveExecutor#ignoreNull() ignoreNull(boolean?)}
     * were not implements, ignore please.</p>
     *
     * @param tableName table name
     * @param where     condition
     * @return Delete executor
     */
    <T> SaveExecutor<T> delete(String tableName, String where);

    /**
     * Entity executor.
     * <p>{@link com.github.chengyuxing.common.anno.Alias @Alias} is optional, use {@link com.github.chengyuxing.common.anno.Alias @Alias} if:</p>
     * <ul>
     *     <li>table name not equals entity class name.</li>
     *     <li>column name not equals entity field name.</li>
     * </ul>
     * <blockquote><pre>
     *     {@code @}Alias("test.user") // table name 'test.user'
     *     public class User{
     *        {@code @}Alias("name") // column name 'name'
     *        private String userName;
     *        private Integer age;  // column name 'age'
     *
     *        // getter...
     *        // setter...
     *     }
     * </pre></blockquote>
     *
     * @param <T> entity type
     * @see com.github.chengyuxing.common.anno.Alias @Alias
     */
    <T> EntitySaveExecutor<T> entity(Class<T> entityClass);

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
    String databaseId();
}
