package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.PagedResource;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Abstract page query config builder.
 */
public abstract class IPageable {
    protected Map<String, Object> args = new HashMap<>();
    protected final String recordQuery;
    protected String countQuery;
    protected final int page;
    protected final int size;
    protected Integer count;
    protected boolean disablePageSql;
    protected Function<Args<Integer>, Args<Integer>> rewriteArgsFunc;
    protected PageHelperProvider pageHelperProvider;

    /**
     * Constructed IPageable with record query, page and size.
     *
     * @param recordQuery record query
     * @param page        current page
     * @param size        page size
     */
    public IPageable(String recordQuery, int page, int size) {
        this.recordQuery = recordQuery;
        this.page = page;
        this.size = size;
    }

    /**
     * Set query args.
     *
     * @param args args
     * @return IPageable
     */
    public IPageable args(Map<String, Object> args) {
        if (args != null) {
            this.args = new HashMap<>(args);
        }
        return this;
    }

    /**
     * Set count query.
     *
     * @param countQuery count query
     * @return IPageable
     */
    public IPageable count(String countQuery) {
        this.countQuery = countQuery;
        return this;
    }

    /**
     * Set data count.
     *
     * @param count data count
     * @return IPageable
     */
    public IPageable count(Integer count) {
        this.count = count;
        return this;
    }

    /**
     * Disable auto generate paged sql ({@link PageHelper#pagedSql(String)} and implement
     * custom count query manually.
     *
     * @param countQuery count query, overwrite {@link #count(String)}
     * @return IPageable
     */
    public IPageable disableDefaultPageSql(String countQuery) {
        this.disablePageSql = true;
        this.countQuery = countQuery;
        return this;
    }

    /**
     * Overwrite paged args: {@link PageHelper#pagedArgs()}<br>
     * e.g. postgresql:
     * <blockquote>
     * <pre>
     * args -&gt; {
     *      args.updateKey("limit", "my_limit");
     *      args.updateKey("offset", "my_offset");
     *      return args;
     * }
     * </pre>
     * </blockquote>
     *
     * @param func paged args rewrite function
     * @return IPageable
     */
    public IPageable rewriteDefaultPageArgs(Function<Args<Integer>, Args<Integer>> func) {
        this.rewriteArgsFunc = func;
        return this;
    }

    /**
     * Set custom page helper provider for current page query.
     *
     * @param pageHelperProvider page helper provider
     * @return IPageable
     * @see #disableDefaultPageSql(String)
     * @see #rewriteDefaultPageArgs(Function)
     */
    public IPageable pageHelper(PageHelperProvider pageHelperProvider) {
        this.pageHelperProvider = pageHelperProvider;
        return this;
    }

    /**
     * Collect paged result.
     *
     * @param mapper paged result each row mapper
     * @param <T>    result type
     * @return paged resource
     */
    public abstract <T> PagedResource<T> collect(Function<DataRow, T> mapper);

    /**
     * Collect paged result.
     *
     * @return paged resource
     */
    public PagedResource<DataRow> collect() {
        return collect(Function.identity());
    }
}
