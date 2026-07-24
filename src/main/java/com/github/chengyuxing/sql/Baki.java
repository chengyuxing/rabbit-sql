package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.plugins.EntityExecutor;
import com.github.chengyuxing.sql.plugins.QueryExecutor;
import com.github.chengyuxing.sql.plugins.SimpleDMLExecutor;
import com.github.chengyuxing.sql.types.BatchResult;
import com.github.chengyuxing.sql.types.DatabaseInfo;
import com.github.chengyuxing.sql.types.Param;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.util.Map;
import java.util.function.Function;

/**
 * <h2>Basic Database Access Interface</h2>
 *
 * <p>This interface provides a high-level abstraction for database operations, including
 * querying, inserting, updating, and deleting data. It is designed to work seamlessly with
 * the Rabbit-SQL IDEA Plugin, which enhances the development experience by providing
 * features like Dynamic SQL tests, auto-completion, and syntax highlighting.</p>
 *
 * <p>The <code>Baki</code> interface is intended to be implemented by classes that provide concrete
 * database access logic. The methods in this interface are designed to
 * be flexible and powerful, allowing for both simple and complex database operations.</p>
 *
 * <p>For more information on how to use this interface, refer to the documentation and
 * examples provided in the Rabbit-SQL project.</p>
 */
public interface Baki {
    /**
     * A constant string used as a key to identify or reference a specific ID within the context of database operations.
     * This key can be utilized in various scenarios such as logging, caching, or uniquely identifying a particular query execution.
     */
    String IDENTIFIER = "_$rabbit.sql.identifier";

    /**
     * Creates a new query executor for the given SQL statement.
     *
     * @param sql The SQL statement to be executed.
     * @return A {@link QueryExecutor} instance that can be used to execute the query and
     * retrieve the results.
     */
    @NotNull QueryExecutor query(@NotNull String sql);

    /**
     * Executes a stored procedure or function.
     *
     * @param procedure The name of the stored procedure or function.
     * @param params    The input, output, and input-output parameters for the procedure.
     * @return A {@link DataRow} containing the result of the procedure.
     */
    @NotNull DataRow call(@NotNull String procedure, Map<String, Param> params);

    /**
     * Executes a SQL statement (DDL, DML, or query).
     *
     * @param sql  The SQL statement to be executed.
     * @param args The arguments to be used in the SQL statement, represented as a map
     *             of parameter names to values.
     * @return A {@link DataRow} containing the result of the execution.
     */
    @NotNull DataRow execute(@NotNull String sql, Map<String, ?> args);

    /**
     * Executes a batch of prepared DML statements.
     *
     * @param sql  The SQL statement to be executed.
     * @param args The arguments to be used in the SQL statement, represented as an
     *             iterable collection of maps, where each map represents a set of arguments.
     * @return The number of rows affected by the execution.
     */
    BatchResult execute(@NotNull String sql, @NotNull Iterable<? extends Map<String, ?>> args);

    /**
     * Executes a batch of prepared DML statements.
     *
     * @param sql       The SQL statement to be executed.
     * @param args      The arguments to be used in the SQL statement, represented as an
     *                  iterable collection of objects.
     * @param argMapper A function that maps each object to a map of parameter names to values.
     * @param <T>       The type of the objects in the data collection.
     * @return The number of rows affected by the execution.
     */
    <T> BatchResult execute(@NotNull String sql, @NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper);

    /**
     * Executes a batch of non-prepared SQL statements (DML or DDL).
     *
     * @param sqlList The SQL statements to be executed, represented as an iterable collection
     *                of strings.
     * @return The number of rows affected by the execution.
     */
    BatchResult execute(@NotNull Iterable<String> sqlList);

    /**
     * Creates a new simple DML executor for the specified table.
     *
     * @param name The name of the table.
     * @return A {@link SimpleDMLExecutor} instance that can be used to perform DML operations
     * on the specified table.
     */
    @NotNull SimpleDMLExecutor table(@NotNull String name);

    /**
     * Creates a new entity executor for the specified entity class.
     *
     * @param clazz The entity class.
     * @param <T>   The type of the entity.
     * @return An {@link EntityExecutor} instance that can be used to perform CRUD operations
     * on the specified entity.
     */
    <T> EntityExecutor<T> entity(@NotNull Class<T> clazz);

    /**
     * Executes a block of code with a connection from the database.
     *
     * @param func A function that takes a {@link Connection} and returns a result.
     * @param <T>  The type of the result.
     * @return The result of the function.
     */
    <T> T using(Function<Connection, T> func);

    /**
     * Retrieves the current database information.
     *
     * @return The information for the current database.
     */
    @NotNull DatabaseInfo databaseInfo();
}
