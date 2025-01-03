package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.dsl.clause.condition.Criteria;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.dsl.types.StandardAggFunction;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Group by clause.
 *
 * @param <T> entity type
 */
public abstract class GroupBy<T> extends ColumnHelper<T> {
    protected Set<String> aggColumns = new LinkedHashSet<>();
    protected Set<String> groupColumns = new LinkedHashSet<>();
    protected List<Criteria> havingCriteria = new ArrayList<>();

    /**
     * Construct a new Group by builder with initial Group by builder.
     *
     * @param clazz entity class
     */
    protected GroupBy(@NotNull Class<T> clazz) {
        super(clazz);
    }

    /**
     * Construct a new Group by builder with initial Group by builder.
     *
     * @param clazz entity class
     * @param other group by builder
     */
    protected GroupBy(@NotNull Class<T> clazz, @NotNull GroupBy<T> other) {
        super(clazz);
        this.aggColumns = other.aggColumns;
        this.groupColumns = other.groupColumns;
        this.havingCriteria = other.havingCriteria;
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

    // It does not return an entity, by default agg columns will be into the query result.
    protected abstract Stream<DataRow> query();

    private void addSelectColumn(String column) {
        if (isIllegalColumn(column)) {
            throw new IllegalArgumentException("Illegal column name: '" + column + "' in group by clause");
        }
        groupColumns.add(column);
    }

    private void addAggColumn(StandardAggFunction aggFunction, String column) {
        if (aggFunction == StandardAggFunction.COUNT) {
            if (column.equals("*")) {
                aggColumns.add(aggFunction.apply(column) + columnWithAlias(aggFunction.getName(), "all"));
                return;
            }
        }
        if (isIllegalColumn(column)) {
            throw new IllegalArgumentException("Illegal agg function column: '" + column + "'");
        }
        aggColumns.add(aggFunction.apply(column) + columnWithAlias(aggFunction.getName(), column));
    }

    private String columnWithAlias(String aggName, String column) {
        return " as " + aggName + "_" + column;
    }

    protected final Set<String> getSelectColumns() {
        Set<String> columns = new LinkedHashSet<>(groupColumns);
        columns.addAll(aggColumns);
        return columns;
    }

    protected final String buildGroupByClause() {
        if (groupColumns.isEmpty()) {
            return "";
        }
        return "\ngroup by " + String.join(", ", groupColumns);
    }
}
