package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.dsl.clause.OrderBy;
import com.github.chengyuxing.sql.dsl.clause.Where;
import com.github.chengyuxing.sql.plugins.PageHelperProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * DSL query operator.
 *
 * @param <T>    entity type
 * @param <SELF> self-type
 */
public interface Query<T, SELF extends Query<T, SELF>> {
    /**
     * Where clause.
     * <p>Support flatten style and nested style to build the complex condition struct.</p>
     * <p>By default, all condition concat with {@code and}, excepted {@link Where#and(Function)}
     * and {@link Where#or(Function)} .</p>
     * SQL:
     * <blockquote><pre>
     * where id &gt; 5 and id &lt; 10 or id in (17, 18, 19)
     * </pre></blockquote>
     * Flatten:
     * <blockquote><pre>
     * .where(g -&gt; g.gt(Guest::getId, 5))
     * .where(g -&gt; g.lt(Guest::getId, 10))
     * .where(g -&gt; g.or(o -&gt; o.in(Guest::getId, Arrays.asList(17, 18, 19))))
     * </pre></blockquote>
     * Nested:
     * <blockquote><pre>
     * .where(g -&gt; g.gt(Guest::getId, 5)
     *             .lt(Guest::getId, 10)
     *             .or(o -&gt; o.in(Guest::getId, Arrays.asList(17, 18, 19))))
     * </pre></blockquote>
     *
     * @param where where builder
     * @return self
     */
    SELF where(@NotNull Function<Where<T>, Where<T>> where);

    /**
     * Order by clause.
     *
     * @param order order by builder
     * @return self
     */
    SELF orderBy(@NotNull Function<OrderBy<T>, OrderBy<T>> order);

    /**
     * Collect result to entity stream (use {@code try-with-resource} to close at the end).
     *
     * @return stream
     */
    Stream<T> stream();

    /**
     * Collect result to list.
     *
     * @return list
     */
    @NotNull List<T> list();

    /**
     * Collect result to list.
     *
     * @param mapper item mapper
     * @param <R>    result type
     * @return list
     */
    @NotNull <R> List<R> list(@NotNull Function<T, R> mapper);

    /**
     * Fetch top number pieces of data.
     *
     * @param n number
     * @return stream
     */
    List<T> top(@Range(from = 1, to = Integer.MAX_VALUE) int n);

    /**
     * Find first item.
     *
     * @return optional first item
     */
    @NotNull Optional<T> findFirst();

    /**
     * Collect result to paged resource.
     *
     * @param page               page number
     * @param size               page size
     * @param pageHelperProvider page helper provider for current.
     * @return paged resource
     */
    @NotNull PagedResource<T> pageable(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                              @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                              @Nullable PageHelperProvider pageHelperProvider);

    /**
     * Collect result to paged resource.
     *
     * @param page page number
     * @param size page size
     * @return paged resource
     */
    @NotNull PagedResource<T> pageable(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                              @Range(from = 1, to = Integer.MAX_VALUE) int size);

    /**
     * Get query result count.
     *
     * @return result count
     */
    @Range(from = 0, to = Long.MAX_VALUE)
    long count();
}
