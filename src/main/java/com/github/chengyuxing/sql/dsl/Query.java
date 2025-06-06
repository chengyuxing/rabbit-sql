package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.dsl.clause.GroupBy;
import com.github.chengyuxing.sql.dsl.clause.OrderBy;
import com.github.chengyuxing.sql.dsl.clause.Where;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.plugins.PageHelperProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
import org.jetbrains.annotations.Unmodifiable;

import java.util.List;
import java.util.Map;
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
     * <p>By default, all condition concat with {@code and}, excepted {@link Where#and(Function, boolean...)}
     * and {@link Where#or(Function, boolean...)} .</p>
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
     * Group by clause.
     * <p>Support complex having clause struct likes {@link #where(Function)}, e.g.</p>
     * SQL:
     * <blockquote><pre>
     * select age,
     *        count(*) as count_all,
     *        max(age) as max_age,
     *        avg(age) as avg_age
     * from ...
     * group by age
     * having count(*) &gt; 1
     * </pre></blockquote>
     * Group by builder:
     * <blockquote><pre>
     * .groupBy(g -&gt; g.count()
     *         .max(Guest::getAge)
     *         .avg(Guest::getAge)
     *         .by(Guest::getAge)
     *         .having(h -&gt; h.count(StandardOperator.GT, 1))
     * )
     * </pre></blockquote>
     *
     * @param group group by builder
     * @return self
     */
    SELF groupBy(@NotNull Function<GroupBy<T>, GroupBy<T>> group);

    /**
     * Order by clause.
     *
     * @param order order by builder
     * @return self
     */
    SELF orderBy(@NotNull Function<OrderBy<T>, OrderBy<T>> order);

    /**
     * Select columns into the result, if empty select will include all column.
     *
     * @param columns the column that without annotation {@link jakarta.persistence.Transient @Transient}
     * @return self
     */
    SELF select(@NotNull List<FieldReference<T>> columns);

    /**
     * Deselect columns into the result.
     *
     * @param columns the column that without annotation {@link jakarta.persistence.Transient @Transient}
     * @return self
     */
    SELF deselect(@NotNull List<FieldReference<T>> columns);

    /**
     * Fetch top number pieces of data.
     *
     * @param n number
     * @return stream
     */
    SELF top(@Range(from = 1, to = Integer.MAX_VALUE) int n);

    /**
     * Collect result to DataRow stream (use {@code try-with-resource} to close at the end).
     *
     * @return stream
     * @see com.github.chengyuxing.sql.support.JdbcSupport#executeQueryStream(String, Map) java8 stream query
     */
    Stream<DataRow> toRowStream();

    /**
     * Collect result to entity stream (use {@code try-with-resource} to close at the end).
     *
     * @return stream
     * @see com.github.chengyuxing.sql.support.JdbcSupport#executeQueryStream(String, Map) java8 stream query
     */
    Stream<T> toStream();

    /**
     * Collect result to list.
     *
     * @return list
     */
    @NotNull List<T> toList();

    /**
     * Collect result to list.
     *
     * @param mapper item mapper
     * @param <R>    result type
     * @return list
     */
    @NotNull <R> List<R> toList(@NotNull Function<T, R> mapper);

    /**
     * Find first item.
     *
     * @return optional first item
     */
    @NotNull Optional<T> findFirst();

    /**
     * Get first item.
     *
     * @return first item
     */
    @Nullable T getFirst();

    /**
     * Collect result to paged resource.
     *
     * @param page               page number
     * @param size               page size
     * @param pageHelperProvider page helper provider for current.
     * @return paged resource
     */
    @NotNull PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                              @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                              @Nullable PageHelperProvider pageHelperProvider);

    /**
     * Collect result to paged resource.
     *
     * @param page page number
     * @param size page size
     * @return paged resource
     */
    @NotNull PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                              @Range(from = 1, to = Integer.MAX_VALUE) int size);

    /**
     * Collect result to paged resource.
     *
     * @param page               page number
     * @param size               page size
     * @param pageHelperProvider page helper provider for current.
     * @return paged resource
     */
    @NotNull PagedResource<DataRow> toPagedRowResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                       @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                                       @Nullable PageHelperProvider pageHelperProvider);

    /**
     * Collect result to paged resource.
     *
     * @param page page number
     * @param size page size
     * @return paged resource
     */
    @NotNull PagedResource<DataRow> toPagedRowResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                       @Range(from = 1, to = Integer.MAX_VALUE) int size);

    /**
     * Check the result exists or not by condition.<br>
     * Notice: Query will not include the {@link #groupBy(Function) Group by clause}.
     *
     * @return true exists otherwise false
     */
    boolean exists();

    /**
     * Get query result count.
     *
     * @return result count
     */
    @Range(from = 0, to = Long.MAX_VALUE)
    long count();

    /**
     * Find first row.
     *
     * @return optional row
     */
    @NotNull Optional<DataRow> findFirstRow();

    /**
     * Get first row.
     *
     * @return first row
     */
    @NotNull DataRow getFirstRow();

    /**
     * Collect to row list.
     *
     * @return list
     */
    @NotNull List<DataRow> toRows();

    /**
     * Collect to map list.
     *
     * @return list
     */
    @NotNull List<Map<String, Object>> toMaps();

    /**
     * Returns the built sql and parameters.
     *
     * @return named parameter sql and parameters
     */
    @NotNull
    @Unmodifiable
    Pair<String, Map<String, Object>> getSql();
}
