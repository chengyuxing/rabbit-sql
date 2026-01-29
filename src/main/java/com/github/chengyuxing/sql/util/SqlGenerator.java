package com.github.chengyuxing.sql.util;

import com.github.chengyuxing.common.util.ValueUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sql generate tool.
 */
public class SqlGenerator {
    private final char namedParamPrefix;
    /**
     * Named parameter pattern.
     */
    private final Pattern namedParamPattern;

    /**
     * Constructs a new SqlGenerator with named parameter prefix.
     *
     * @param namedParamPrefix named parameter prefix
     */
    public SqlGenerator(char namedParamPrefix) {
        if (namedParamPrefix == ' ') {
            throw new IllegalArgumentException("Prefix char cannot be empty.");
        }
        this.namedParamPrefix = namedParamPrefix;
        this.namedParamPattern = Pattern.compile(String.format(
                        "(?<!\\%1$s)\\%1$s(%2$s)|" +        // Named parameters
                                "'(?:''|[^'])*'|" +         // String literals
                                "\"(?:\"\"|[^\"])*\"|" +    // Identifier literals
                                "--.*?$|" +                 // Line comments
                                "/\\*.*?\\*/|" +            // Block comments
                                "::\\w+",                   // PostgreSQL type casts
                        namedParamPrefix, ValueUtils.VAR_PATH_EXPRESSION_PATTERN.pattern()),
                Pattern.DOTALL | Pattern.MULTILINE
        );
    }

    /**
     * Generated sql meta data.
     */
    public static final class PreparedSqlMetaData {
        private final String sourceSql;
        private final String prepareSql;
        private final Map<String, List<Integer>> argNameIndexMapping;
        private final Map<String, ?> args;

        /**
         * Construct a new GeneratedSqlMetaData instance.
         *
         * @param sourceSql           named parameter sql
         * @param prepareSql          prepared SQL
         * @param argNameIndexMapping prepared SQL arg name index mapping
         * @param args                args
         */
        public PreparedSqlMetaData(String sourceSql, String prepareSql, Map<String, List<Integer>> argNameIndexMapping, Map<String, ?> args) {
            this.sourceSql = sourceSql;
            this.prepareSql = prepareSql;
            this.argNameIndexMapping = argNameIndexMapping;
            this.args = args;
        }

        public String getSourceSql() {
            return sourceSql;
        }

        public String getPrepareSql() {
            return prepareSql;
        }

        public Map<String, List<Integer>> getArgNameIndexMapping() {
            return argNameIndexMapping;
        }

        public Map<String, ?> getArgs() {
            return args;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PreparedSqlMetaData)) return false;

            PreparedSqlMetaData that = (PreparedSqlMetaData) o;
            return Objects.equals(getSourceSql(), that.getSourceSql()) && Objects.equals(getPrepareSql(), that.getPrepareSql()) && Objects.equals(getArgNameIndexMapping(), that.getArgNameIndexMapping()) && Objects.equals(getArgs(), that.getArgs());
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(getSourceSql());
            result = 31 * result + Objects.hashCode(getPrepareSql());
            result = 31 * result + Objects.hashCode(getArgNameIndexMapping());
            result = 31 * result + Objects.hashCode(getArgs());
            return result;
        }
    }

    /**
     * Generate prepared SQL by named parameter SQL, e.g.
     * <p>before: </p>
     * <blockquote>
     * <pre>select * from table where id = :id</pre>
     * </blockquote>
     * <p>after: </p>
     * <blockquote>
     * <pre>select * from table where id = ?</pre>
     * </blockquote>
     *
     * @param sql  named parameter sql
     * @param args data of named parameter
     * @return GeneratedSqlMetaData
     */
    public PreparedSqlMetaData generatePreparedSql(final String sql, Map<String, ?> args) {
        Map<String, List<Integer>> indexMap = new HashMap<>();
        Matcher matcher = namedParamPattern.matcher(sql);
        int index = 1;
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String name = matcher.group(1);
            String replacement;
            if (name != null) {
                replacement = "?";
                indexMap.computeIfAbsent(name, k -> new ArrayList<>()).add(index);
                index++;
            } else {
                replacement = matcher.group();
            }
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        return new PreparedSqlMetaData(sql, buffer.toString(), indexMap, args);
    }

    /**
     * Generate normal SQL by named parameter SQL.
     *
     * @param sql                 named parameter sql
     * @param args                data of named parameter
     * @param namedParamFormatter named param string literal formatter
     * @return normal SQL
     */
    public String generateSql(final String sql, Map<String, ?> args, Function<Object, String> namedParamFormatter) {
        StringBuffer buffer = new StringBuffer();
        Matcher matcher = namedParamPattern.matcher(sql);
        while (matcher.find()) {
            String name = matcher.group(1);
            String replacement;
            if (name != null) {
                Object value = ValueUtils.getDeepValue(args, name);
                replacement = namedParamFormatter.apply(value);
            } else {
                replacement = matcher.group();
            }
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    /**
     * Generate named parameter insert statement.
     *
     * @param tableName table name
     * @param columns   table columns
     * @return named parameter insert statement
     */
    public String generateNamedParamInsert(@NotNull final String tableName, @NotNull Collection<String> columns) {
        if (columns.isEmpty()) {
            return "insert into " + tableName + " default values";
        }
        StringJoiner f = new StringJoiner(", ");
        StringJoiner h = new StringJoiner(", ");
        for (String column : columns) {
            SqlUtils.assertInvalidIdentifier(column);
            f.add(column);
            h.add(namedParamPrefix + column);
        }
        return "insert into " + tableName + "(" + f + ") values (" + h + ")";
    }

    /**
     * Generate named parameter update by statement.
     *
     * @param tableName table name
     * @param columns   table columns
     * @return update by statement
     */
    public String generateNamedParamUpdateBy(String tableName, Collection<String> columns) {
        StringJoiner sb = new StringJoiner(",\n\t");
        for (String column : columns) {
            SqlUtils.assertInvalidIdentifier(column);
            sb.add(column + " = " + namedParamPrefix + column);
        }
        return "update " + tableName + "\nset " + sb + "\nwhere ";
    }

    /**
     * Generate delete by statement.
     *
     * @param tableName table name
     * @return delete by statement
     */
    public String generateDeleteBy(String tableName) {
        return "delete from " + tableName + " where ";
    }

    /**
     * Generate select all columns statement
     *
     * @param tableName table name
     * @return select columns statement
     */
    public String generateColumnsQueryStatement(String tableName) {
        return "select * from " + tableName + " where 1 = 2";
    }

    public Pattern getNamedParamPattern() {
        return namedParamPattern;
    }

    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }
}
