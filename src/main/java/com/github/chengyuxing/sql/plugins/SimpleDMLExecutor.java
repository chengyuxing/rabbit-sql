package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Simple sql builder executor.
 */
public interface SimpleDMLExecutor {
    /**
     * Execute insert.
     *
     * @param data data
     * @return affected row count
     */
    int insert(@NotNull Map<String, ?> data);

    /**
     * Execute batch insert.
     *
     * @param data data
     * @return affected row count
     */
    int insert(@NotNull Iterable<? extends Map<String, ?>> data);

    /**
     * Execute batch insert.
     *
     * @param data      data
     * @param argMapper arg mapping to Map function
     * @param <T>       arg type
     * @return affected row count
     */
    <T> int insert(@NotNull Iterable<T> data, @NotNull Function<T, ? extends Map<String, ?>> argMapper);

    /**
     * Where conditional interface.
     *
     * @param condition condition e.g. id = :id
     * @return Conditional object
     */
    Conditional where(@NotNull String condition);

    /**
     * Execute query table fields.
     *
     * @return fields
     */
    List<String> fields();

    /**
     * The dml statement with where condition.
     */
    interface Conditional {
        /**
         * Execute Update, generate update statement by args, e.g.
         * <blockquote>
         * <pre>
         *  args： {id:14, name:'cyx', address:'kunming'}
         *  where condition："id = :id"
         *  generate：update{@code <table>} set name = :name, address = :address
         *       where id = :id
         *  </pre>
         * </blockquote>
         *
         * @param args all args which contains sets and conditional part
         * @return affected row count
         */
        int update(@NotNull Map<String, ?> args);

        /**
         * Execute batch update, generate update statement by 1st row of args, e.g.
         * <blockquote>
         * <pre>
         *  args： {id:14, name:'cyx', address:'kunming'},{...}...
         *  where condition："id = :id"
         *  generate：update{@code <table>} set name = :name, address = :address
         *       where id = :id
         *  </pre>
         * </blockquote>
         * Notice: where condition must contain at least 1 named parameter and args must contain its value.
         *
         * @param args all args which contains sets and conditional part
         * @return affected row count
         */
        int update(@NotNull Iterable<? extends Map<String, ?>> args);

        /**
         * Execute batch update, generate update statement by 1st row of args
         *
         * @param args      all args which contains sets and conditional part
         * @param argMapper arg mapping to Map function
         * @param <T>       arg type
         * @return affected row count
         * @see #update(Iterable)
         */
        <T> int update(@NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper);

        /**
         * Execute delete.
         *
         * @param args args
         * @return affected row count
         */
        int delete(@NotNull Map<String, ?> args);

        /**
         * Execute batch delete.
         *
         * @param args args
         * @return affected row count
         */
        int delete(@NotNull Iterable<? extends Map<String, ?>> args);

        /**
         * Execute batch delete.
         *
         * @param args      args
         * @param argMapper arg mapping to Map function
         * @param <T>       arg type
         * @return affected row count
         */
        <T> int delete(@NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper);
    }
}
