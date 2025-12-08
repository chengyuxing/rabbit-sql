package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/**
 * Simple sql builder executor.
 */
public interface SimpleDMLExecutor {

    int insert(@NotNull Map<String, Object> data);

    int insert(@NotNull Iterable<? extends Map<String, Object>> data);

    /**
     * Where conditional interface.
     *
     * @param condition condition e.g. id = :id
     * @return Conditional object
     */
    Conditional where(@NotNull String condition);

    /**
     * Query table fields.
     *
     * @return fields
     */
    List<String> fields();

    interface Conditional {
        /**
         * Update, generate update statement by args, e.g.
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
        int update(@NotNull Map<String, Object> args);

        /**
         * Batch update, generate update statement by 1st row of args, e.g.
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
        int update(@NotNull Iterable<? extends Map<String, Object>> args);

        int delete(@NotNull Map<String, Object> args);

        int delete(@NotNull Iterable<? extends Map<String, Object>> args);
    }
}
