package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.dsl.types.StandardAggFunction;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;

/**
 * Group by clause.
 *
 * @param <T> entity type
 */
public abstract class GroupBy<T> extends ColumnHelper<T> {
    protected final Set<Pair<String, String>> aggColumns = new LinkedHashSet<>();
    protected final Set<String> groupColumns = new LinkedHashSet<>();

    /**
     * Construct a new Group by builder with initial Group by builder.
     *
     * @param clazz entity class
     */
    protected GroupBy(@NotNull Class<T> clazz) {
        super(clazz);
    }

    /**
     * {@code count(*)}
     *
     * @return group by builder
     */
    public GroupBy<T> count() {
        addAggColumn(StandardAggFunction.COUNT, "*");
        return this;
    }

    /**
     * {@code count([column])}
     *
     * @param column column
     * @return group by builder
     */
    public GroupBy<T> count(FieldReference<T> column) {
        addAggColumn(StandardAggFunction.COUNT, getColumnName(column));
        return this;
    }

    /**
     * {@code sum([column])}
     *
     * @param column column
     * @return group by builder
     */
    public GroupBy<T> sum(FieldReference<T> column) {
        addAggColumn(StandardAggFunction.SUM, getColumnName(column));
        return this;
    }

    /**
     * {@code max([column])}
     *
     * @param column column
     * @return group by builder
     */
    public GroupBy<T> max(FieldReference<T> column) {
        addAggColumn(StandardAggFunction.MAX, getColumnName(column));
        return this;
    }

    /**
     * {@code min([column])}
     *
     * @param column column
     * @return group by builder
     */
    public GroupBy<T> min(FieldReference<T> column) {
        addAggColumn(StandardAggFunction.MIN, getColumnName(column));
        return this;
    }

    /**
     * {@code avg([column])}
     *
     * @param column column
     * @return group by builder
     */
    public GroupBy<T> avg(FieldReference<T> column) {
        addAggColumn(StandardAggFunction.AVG, getColumnName(column));
        return this;
    }

    /**
     * group by columns.
     *
     * @param column column
     * @param more   more column
     * @return group by builder
     */
    @SafeVarargs
    public final GroupBy<T> by(FieldReference<T> column, FieldReference<T>... more) {
        addSelectColumn(getColumnName(column));
        for (FieldReference<T> c : more) {
            addSelectColumn(getColumnName(c));
        }
        return this;
    }

    /**
     * Providers a Having instance to built having clause.
     *
     * @param having having instance
     * @return group by builder
     */
    public abstract GroupBy<T> having(Function<Having<T>, Having<T>> having);

    private void addSelectColumn(String column) {
        if (isIllegalColumn(column)) {
            throw new IllegalArgumentException("Illegal column name: '" + column + "' in group by clause");
        }
        groupColumns.add(column);
    }

    private void addAggColumn(StandardAggFunction aggFunction, String column) {
        if (aggFunction == StandardAggFunction.COUNT) {
            if (column.equals("*")) {
                aggColumns.add(Pair.of(aggFunction.apply(column), columnAlias(aggFunction.getName(), "all")));
                return;
            }
        }
        if (isIllegalColumn(column)) {
            throw new IllegalArgumentException("Illegal agg function column: '" + column + "'");
        }
        aggColumns.add(Pair.of(aggFunction.apply(column), columnAlias(aggFunction.getName(), column)));
    }

    private String columnAlias(String aggName, String column) {
        return aggName + "_" + column;
    }

    protected final @NotNull Set<String> getSelectColumns() {
        Set<String> columns = new LinkedHashSet<>(groupColumns);
        for (Pair<String, String> p : aggColumns) {
            columns.add(p.getItem1() + " as " + p.getItem2());
        }
        return columns;
    }

    protected final String build() {
        if (groupColumns.isEmpty()) {
            return "";
        }
        return "\ngroup by " + String.join(", ", groupColumns);
    }
}
