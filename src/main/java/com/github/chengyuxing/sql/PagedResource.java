package com.github.chengyuxing.sql;

import com.github.chengyuxing.sql.page.PageHelper;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Paged resource object.
 *
 * @param <T> data type
 */
public record PagedResource<T>(PageHelper pager, List<T> data) {

    /**
     * Returns a PagedResource.
     *
     * @param pager page helper instance
     * @param data  paged data
     * @param <T>   data type
     * @return PagedResource instance
     */
    public static <T> PagedResource<T> of(@NotNull PageHelper pager, List<T> data) {
        return new PagedResource<>(pager, data);
    }

    /**
     * Returns an empty PagedResource.
     *
     * @param <T> data type
     * @return empty PagedResource
     */
    public static <T> PagedResource<T> empty() {
        return of(new PageHelper() {
            @Override
            public @NotNull String pagedSql(char namedParamPrefix, @NotNull String sql) {
                return "";
            }

            @Override
            public @NotNull Args<Integer> pagedArgs() {
                return Args.of();
            }
        }, Collections.emptyList());
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
}
