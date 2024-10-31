package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.dsl.clause.condition.Criteria;
import com.github.chengyuxing.sql.dsl.type.ColumnReference;
import com.github.chengyuxing.sql.dsl.type.StandardAggFunction;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class GroupBy<T> extends ColumnHelper<T> {
    protected Set<String> aggColumns = new LinkedHashSet<>();
    protected Set<String> groupColumns = new LinkedHashSet<>();
    protected List<Criteria> havingCriteria = new ArrayList<>();

    protected GroupBy(@NotNull Class<T> clazz) {
        super(clazz);
    }

    protected GroupBy(@NotNull Class<T> clazz, @NotNull GroupBy<T> other) {
        super(clazz);
        this.aggColumns = other.aggColumns;
        this.groupColumns = other.groupColumns;
        this.havingCriteria = other.havingCriteria;
    }

    public GroupBy<T> count() {
        addAggColumn(StandardAggFunction.COUNT, "*");
        return this;
    }

    public GroupBy<T> count(ColumnReference<T> columnReference) {
        addAggColumn(StandardAggFunction.COUNT, getColumnName(columnReference));
        return this;
    }

    public GroupBy<T> sum(ColumnReference<T> columnReference) {
        addAggColumn(StandardAggFunction.SUM, getColumnName(columnReference));
        return this;
    }

    public GroupBy<T> max(ColumnReference<T> columnReference) {
        addAggColumn(StandardAggFunction.MAX, getColumnName(columnReference));
        return this;
    }

    public GroupBy<T> min(ColumnReference<T> columnReference) {
        addAggColumn(StandardAggFunction.MIN, getColumnName(columnReference));
        return this;
    }

    public GroupBy<T> avg(ColumnReference<T> columnReference) {
        addAggColumn(StandardAggFunction.AVG, getColumnName(columnReference));
        return this;
    }

    @SafeVarargs
    public final GroupBy<T> by(ColumnReference<T> column, ColumnReference<T>... more) {
        addSelectColumn(getColumnName(column));
        for (ColumnReference<T> c : more) {
            addSelectColumn(getColumnName(c));
        }
        return this;
    }

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
