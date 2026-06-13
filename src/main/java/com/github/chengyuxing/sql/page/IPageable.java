package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.common.DataRow;
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
    protected final Map<String, Object> args = new HashMap<>();
    protected final String recordQuery;
    protected final int page;
    protected final int size;
    protected String countQuery;
    protected Integer count;
    protected boolean disablePageSql;
    protected String startNumKey;
    protected String endNumKey;
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
            this.args.putAll(args);
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
     * <p>
     * It's necessary to overwrite the default page params [{@link PageHelper#START_NUM_KEY start},
     * {@link PageHelper#END_NUM_KEY end}] for your custom param name in the sql.
     * <p>
     * The actually page SQL is in a sub or view query:
     * <blockquote><pre>
     *     with cte as (select * from table limit :length offset :index)
     *     select * from cte
     * </pre></blockquote>
     * To overwrite it e.g:
     * <blockquote><pre>
     *     disableDefaultPageSql("&myQueryCount", "length", "index")
     * </pre></blockquote>
     * Two meaning of the params in the different page type:
     *
     * <ul>
     *     <li>{@link PageHelper#START_NUM_KEY start} number to {@link PageHelper#END_NUM_KEY end} number</li>
     *     <li>{@link PageHelper#START_NUM_KEY left} number and {@link PageHelper#END_NUM_KEY right} number</li>
     * </ul>
     *
     * @param countQuery  count query, overwrite {@link #count(String)}
     * @param startNumKey the pageable param start number key name
     * @param endNumKey   the pageable param end number key name
     * @return IPageable
     */
    public IPageable disableDefaultPageSql(String countQuery, String startNumKey, String endNumKey) {
        this.disablePageSql = true;
        this.countQuery = countQuery;
        this.startNumKey = startNumKey;
        this.endNumKey = endNumKey;
        return this;
    }

    /**
     * Set custom page helper provider for current page query.
     *
     * @param pageHelperProvider page helper provider
     * @return IPageable
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
