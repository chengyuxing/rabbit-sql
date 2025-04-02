package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.IPageable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Query executor.
 */
public abstract class QueryExecutor {
    protected final String sql;
    protected final Map<String, Object> args = new HashMap<>();

    /**
     * Constructs a new Query with 1 sql.
     *
     * @param sql named parameter sql or sql name
     */
    public QueryExecutor(@NotNull String sql) {
        this.sql = sql;
    }

    /**
     * Overwrite and set new args.
     *
     * @param args args
     * @return Query
     * @see com.github.chengyuxing.sql.Args Args&lt;Object&gt;
     */
    public QueryExecutor args(Map<String, Object> args) {
        if (args != null) {
            this.args.putAll(args);
        }
        return this;
    }

    /**
     * Overwrite and set new args.
     *
     * @param keyValues multi pair of key-value data
     * @return Query
     */
    public QueryExecutor args(Object... keyValues) {
        if (keyValues.length > 0) {
            this.args.putAll(Args.of(keyValues));
        }
        return this;
    }

    /**
     * Add arg.
     *
     * @param key   key
     * @param value value
     * @return Query
     */
    public QueryExecutor arg(String key, Object value) {
        this.args.put(key, value);
        return this;
    }

    /**
     * Collect result to Stream.
     *
     * @return Stream query result
     */
    public abstract Stream<DataRow> stream();

    /**
     * Collect result to maps.
     *
     * @return maps
     */
    public abstract List<Map<String, Object>> maps();

    /**
     * Collect result to rows.
     *
     * @return rows
     */
    public abstract List<DataRow> rows();

    /**
     * Collect result to entities.
     *
     * @param entityClass entity class
     * @param <T>         entity type
     * @return entities
     */
    public abstract <T> List<T> entities(Class<T> entityClass);

    /**
     * Convert state to page query.
     * <p>If built-in paged sql not enough, such as postgresql's view query:</p>
     * <blockquote>
     * <pre>with a as (select ... limit 0 offset 5)<br>select * from a;</pre></blockquote>
     * <p>About custom page query config:</p>
     * <ul>
     * <li>custom count query: {@link IPageable#count(String) count(String)};</li>
     * <li>disable auto generate paged sql: {@link IPageable#disableDefaultPageSql(String) disableDefaultPageSql(String)}, otherwise above example will append ({@code limit ... offset ...}) to the end;</li>
     * <li>custom page args name: {@link IPageable#rewriteDefaultPageArgs(Function) rewriteDefaultPageArgs(Function)}.</li>
     * </ul>
     *
     * @param page current page
     * @param size page size
     * @return IPageable instance
     */
    public abstract IPageable pageable(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                       @Range(from = 1, to = Integer.MAX_VALUE) int size);

    /**
     * Convert state to page query.
     *
     * @param pageKey page number key name which in args
     * @param sizeKey page size key name which in args
     * @return IPageable instance
     * @see #pageable(int, int)
     */
    public abstract IPageable pageable(@NotNull String pageKey, @NotNull String sizeKey);

    /**
     * Convert state to page query.
     *
     * @return IPageable instance
     * @see #pageable(String, String)
     */
    public abstract IPageable pageable();

    /**
     * Collect 1st row by page query.
     *
     * @return 1st row or empty row
     * @see #findFirst()
     */
    public abstract @NotNull DataRow findFirstRow();

    /**
     * Collect 1st entity by page query.
     *
     * @param entityClass entity class
     * @param <T>         entity type
     * @return 1st entity or null
     * @see #findFirst()
     */
    public abstract @Nullable <T> T findFirstEntity(Class<T> entityClass);

    /**
     * Collect 1st optional row.
     *
     * @return 1st optional row
     */
    public abstract Optional<DataRow> findFirst();
}
