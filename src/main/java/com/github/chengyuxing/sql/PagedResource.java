package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.page.PageHelper;

import java.util.Collections;
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
     * Returns a PagedResource.
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
     * Returns an empty PagedResource.
     *
     * @param <T> data type
     * @return empty PagedResource
     */
    public static <T> PagedResource<T> empty() {
        return of(null, Collections.emptyList());
    }

    /**
     * Convert paged resource to custom structured result,  e.g.
     * <blockquote>
     * <pre>
     * ({@link PageHelper pager}, data) -&gt; {@link com.github.chengyuxing.common.DataRow DataRow}.of(
     *                    "length", pager.getRecordCount(),
     *                    "data", data)
     *                  );
     * </pre>
     * </blockquote>
     *
     * @param converter (PageHelper, List) -&gt; (new result)
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PagedResource)) return false;

        PagedResource<?> that = (PagedResource<?>) o;

        if (getPager() != null ? !getPager().equals(that.getPager()) : that.getPager() != null) return false;
        return getData() != null ? getData().equals(that.getData()) : that.getData() == null;
    }

    @Override
    public int hashCode() {
        int result = getPager() != null ? getPager().hashCode() : 0;
        result = 31 * result + (getData() != null ? getData().hashCode() : 0);
        return result;
    }
}
