package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.dsl.clause.GroupBy;
import com.github.chengyuxing.sql.dsl.clause.OrderBy;
import com.github.chengyuxing.sql.dsl.clause.Where;
import com.github.chengyuxing.sql.dsl.type.FieldReference;
import com.github.chengyuxing.sql.page.PageHelperProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
import org.jetbrains.annotations.Unmodifiable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * DSL query operator.
 *
 * @param <T>    entity type
 * @param <SELF> self type
 */
public interface Query<T, SELF extends Query<T, SELF>> {
    /**
     * Where clause.
     *
     * @param where where builder
     * @return self
     */
    SELF where(@NotNull Function<Where<T>, Where<T>> where);

    /**
     * Group by clause.
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
     * Select columns into the result which the field ,if empty select will include all column.
     *
     * @param columns the column that without annotation {@link javax.persistence.Transient @Transient}
     * @return self
     */
    SELF select(@NotNull List<FieldReference<T>> columns);

    /**
     * Check the built result currently.
     *
     * @param consumer (sql, (name parameter sql, params)) -&gt; _
     * @return self
     */
    SELF peek(@NotNull BiConsumer<String, Pair<String, Map<String, Object>>> consumer);

    /**
     * Collect result to DataRow stream (use {@code try-with-resource} to close at the end).
     *
     * @return stream
     * @see com.github.chengyuxing.sql.support.JdbcSupport#executeQueryStream(String, Map) java8 stream query
     */
    Stream<DataRow> toRowStream();

    /**
     * Collect result to DataRow stream (use {@code try-with-resource} to close at the end).
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
    List<T> toList();

    /**
     * Collect result to list.
     *
     * @param mapper item mapper
     * @param <R>    result type
     * @return list
     */
    <R> List<R> toList(@NotNull Function<T, R> mapper);

    /**
     * Collect.
     *
     * @param mapper    item mapper
     * @param collector result collector
     * @param <R>       result type
     * @param <V>       mapped value type
     * @return any
     */
    <R, V> R collect(@NotNull Function<T, V> mapper, @NotNull Collector<V, ?, R> collector);

    /**
     * Collect.
     *
     * @param collector result collector
     * @param <R>       result type
     * @return any
     */
    <R> R collect(@NotNull Collector<T, ?, R> collector);

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
     * Collect.
     *
     * @param mapper    item mapper
     * @param collector result collector
     * @param <R>       result type
     * @param <V>       mapped value type
     * @return any
     */
    <R, V> R collectRow(@NotNull Function<DataRow, V> mapper, @NotNull Collector<V, ?, R> collector);

    /**
     * Collect.
     *
     * @param collector result collector
     * @param <R>       result type
     * @return any
     */
    <R> R collectRow(@NotNull Collector<DataRow, ?, R> collector);

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
    List<DataRow> toRows();

    /**
     * Collect to map list.
     *
     * @return list
     */
    List<Map<String, Object>> toMaps();

    /**
     * Returns the built sql and parameters.
     *
     * @return named parameter sql and parameters
     */
    @NotNull
    @Unmodifiable
    Pair<String, Map<String, Object>> getSql();
}
