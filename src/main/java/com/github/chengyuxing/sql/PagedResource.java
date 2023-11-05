package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.page.PageHelper;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Paged resource object.
 *
 * @param <T> data type
 */
public final class PagedResource<T> {
    private PageHelper pager;
    private List<T> data;

    public PagedResource(PageHelper pager, List<T> data) {
        this.pager = pager;
        this.data = data;
    }

    /**
     * Constructs a PagedResource.
     *
     * @param pager page helper instance
     * @param data  paged data
     * @param <T>   data type
     * @return PagedResource instance
     */
    public static <T> PagedResource<T> of(PageHelper pager, List<T> data) {
        return new PagedResource<>(pager, data);
    }

    /**
     * Convert paged resource to custom structured result,  e.g.
     * <blockquote>
     * <pre>
     * (pager, data) -> {@link com.github.chengyuxing.common.DataRow DataRow}.of(
     *                    "length", pager.getRecordCount(),
     *                    "data", data)
     *                  );
     * </pre>
     * </blockquote>
     *
     * @param converter converter [PageHelper, PagedResource] {@code ->} result
     * @param <R>       result type
     * @return new structured result
     */
    public <R> R to(BiFunction<PageHelper, List<T>, R> converter) {
        return converter.apply(pager, data);
    }

    void setData(List<T> data) {
        this.data = data;
    }

    void setPager(PageHelper pager) {
        this.pager = pager;
    }

    public List<T> getData() {
        return data;
    }

    public PageHelper getPager() {
        return pager;
    }

    @Override
    public String toString() {
        return "Pageable{" +
                "pager=" + pager +
                ", data=" + data +
                '}';
    }
}
