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

public interface Query<T, SELF extends Query<T, SELF>> {
    SELF where(@NotNull Function<Where<T>, Where<T>> where);

    SELF groupBy(@NotNull Function<GroupBy<T>, GroupBy<T>> group);

    SELF orderBy(@NotNull Function<OrderBy<T>, OrderBy<T>> order);

    SELF select(@NotNull List<FieldReference<T>> columns);

    SELF peek(@NotNull BiConsumer<String, Pair<String, Map<String, Object>>> consumer);

    Stream<DataRow> toRowStream();

    Stream<T> toStream();

    List<T> toList();

    <R> List<R> toList(@NotNull Function<T, R> mapper);

    <R, V> R collect(@NotNull Function<T, V> func, @NotNull Collector<V, ?, R> collector);

    <R> R collect(@NotNull Collector<T, ?, R> collector);

    @NotNull Optional<T> findFirst();

    @Nullable T getFirst();

    @NotNull PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                              @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                              @Nullable PageHelperProvider pageHelperProvider);

    @NotNull PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                              @Range(from = 1, to = Integer.MAX_VALUE) int size);

    @NotNull PagedResource<DataRow> toPagedRowResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                       @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                                       @Nullable PageHelperProvider pageHelperProvider);

    @NotNull PagedResource<DataRow> toPagedRowResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                       @Range(from = 1, to = Integer.MAX_VALUE) int size);

    boolean exists();

    @Range(from = 0, to = Long.MAX_VALUE)
    long count();

    <R, V> R collectRow(@NotNull Function<DataRow, V> func, @NotNull Collector<V, ?, R> collector);

    <R> R collectRow(@NotNull Collector<DataRow, ?, R> collector);

    @NotNull Optional<DataRow> findFirstRow();

    @NotNull DataRow getFirstRow();

    List<DataRow> toRows();

    List<Map<String, Object>> toMaps();

    @NotNull
    @Unmodifiable
    Pair<String, Map<String, Object>> getSql();
}
