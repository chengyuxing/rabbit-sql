package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.plugins.PageHelperProvider;
import org.jetbrains.annotations.Range;

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
     * Constructs an IPageable with record query, page and size.
     *
     * @param recordQuery record query
     * @param page        current page
     * @param size        page size
     */
    public IPageable(String recordQuery,
                     @Range(from = 1, to = Integer.MAX_VALUE) int page,
                     @Range(from = 1, to = Integer.MAX_VALUE) int size) {
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
    public IPageable count(@Range(from = 0, to = Integer.MAX_VALUE) Integer count) {
        this.count = count;
        return this;
    }

    /**
     * Disable auto generate paged sql ({@link PageHelper#pagedSql(char, String)} and implement
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
     *      args.updateKey({@link PageHelper#START_NUM_KEY START_NUM_KEY}, "my_limit");
     *      args.updateKey({@link PageHelper#END_NUM_KEY END_NUM_KEY}, "my_offset");
     *      return args;
     * }
     * </pre>
     * </blockquote>
     *
     * @param func (old paged args) -&gt; (new paged args)
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
     * @param mapper (each row) -&gt; (each any)
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
