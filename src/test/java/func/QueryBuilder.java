package func;

import func.ops.*;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {
    protected final String tableName;
    protected String fields = "*";
    public DynamicSqlBuilder dynamicSqlBuilder;

    public QueryBuilder(String tableName) {
        this.tableName = tableName;
    }

    public QueryBuilder fields(String... fields) {
        this.fields = String.join(", ", fields);
        return this;
    }

    public DynamicSqlBuilder where() {
        return new DynamicSqlBuilder(this);
    }

    public static class DynamicSqlBuilder {
        private final QueryBuilder queryBuilder;

        List<String> ifs = new ArrayList<>();

        public DynamicSqlBuilder(QueryBuilder queryBuilder) {
            this.queryBuilder = queryBuilder;
        }

        public DynamicSqlBuilder if_(String expression, String sql) {
            ifs.add(new If(expression, sql).toString());
            return this;
        }

        public DynamicSqlBuilder if_(String expression, If if_) {
            ifs.add(new If(expression, if_).toString());
            return this;
        }

        public DynamicSqlBuilder switch_(String key, Case case_, Case... more) {
            ifs.add(new Switch(key, case_, more).toString());
            return this;
        }

        public DynamicSqlBuilder choose(When when, When... more) {
            ifs.add(new Choose(when, more).toString());
            return this;
        }

        @Override
        public String toString() {
            return String.join("\n", ifs);
        }

        public QueryBuilder build() {
            queryBuilder.dynamicSqlBuilder = this;
            return queryBuilder;
        }
    }

    @Override
    public String toString() {
        return "select " + fields + " from " + tableName + " where\n" + dynamicSqlBuilder.toString();
    }
}
