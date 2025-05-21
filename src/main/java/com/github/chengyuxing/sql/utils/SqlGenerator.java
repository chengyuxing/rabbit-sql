package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.script.expression.Patterns;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.dsl.types.StandardOperator;
import com.github.chengyuxing.sql.plugins.NamedParamFormatter;
import com.github.chengyuxing.sql.plugins.TemplateFormatter;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.sql.utils.SqlUtil.*;

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
     * Default implementation: {@link SqlUtil#parseValue(Object, boolean) parseValue(value, true)}
     */
    private NamedParamFormatter namedParamFormatter = v -> parseValue(v, true);
    /**
     * Sql template ({@code ${[!]key}}) formatter.
     * Default implementation: {@link SqlUtil#parseValue(Object, boolean) parseValue(value, boolean)}
     */
    private TemplateFormatter templateFormatter = SqlUtil::parseValue;

    /**
     * Constructs a new SqlGenerator with named parameter prefix.
     *
     * @param namedParamPrefix named parameter prefix
     */
    public SqlGenerator(char namedParamPrefix) {
        if (namedParamPrefix == ' ') {
            throw new IllegalArgumentException("prefix char cannot be empty.");
        }
        this.namedParamPrefix = namedParamPrefix;
        this.namedParamPattern = Pattern.compile(String.format(
                        "(?<!\\%1$s)\\%1$s(%2$s)|" +        // Named parameters
                                "'(?:''|[^'])*'|" +         // String literals
                                "--.*?$|" +                 // Line comments
                                "/\\*.*?\\*/|" +            // Block comments
                                "::\\w+",                   // PostgreSQL type casts
                        namedParamPrefix, Patterns.VAR_KEY_PATTERN),
                Pattern.DOTALL | Pattern.MULTILINE
        );
    }

    /**
     * Generated sql meta data.
     */
    public static final class GeneratedSqlMetaData {
        private final String sourceSql;
        private final String prepareSql;
        private final Map<String, List<Integer>> argNameIndexMapping;
        private final Map<String, Object> args;

        /**
         * Construct a new GeneratedSqlMetaData instance.
         *
         * @param sourceSql           named parameter sql
         * @param prepareSql          prepared sql
         * @param argNameIndexMapping prepared sql arg name index mapping
         * @param args                args
         */
        public GeneratedSqlMetaData(String sourceSql, String prepareSql, Map<String, List<Integer>> argNameIndexMapping, Map<String, Object> args) {
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

        public Map<String, Object> getArgs() {
            return args;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GeneratedSqlMetaData)) return false;

            GeneratedSqlMetaData that = (GeneratedSqlMetaData) o;
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
     * Generate prepared sql by named parameter sql, e.g.
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
    public GeneratedSqlMetaData generatePreparedSql(final String sql, Map<String, Object> args) {
        Map<String, List<Integer>> indexMap = new HashMap<>();
        Matcher matcher = namedParamPattern.matcher(sql);
        int index = 1;
        StringBuilder buffer = new StringBuilder();
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
        return new GeneratedSqlMetaData(sql, buffer.toString(), indexMap, args);
    }

    /**
     * Generate normal sql by named parameter sql.
     *
     * @param sql  named parameter sql
     * @param args data of named parameter
     * @return normal sql
     * @see #setNamedParamFormatter(NamedParamFormatter)
     * @see #setTemplateFormatter(TemplateFormatter)
     */
    public String generateSql(final String sql, Map<String, ?> args) {
        StringBuilder buffer = new StringBuilder();
        Matcher matcher = namedParamPattern.matcher(sql);
        while (matcher.find()) {
            String name = matcher.group(1);
            String replacement;
            if (name != null) {
                Object value = name.contains(".") ? ObjectUtil.getDeepValue(args, name) : args.get(name);
                replacement = namedParamFormatter.format(value);
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
     * @param tableName  table name
     * @param data       data
     * @param columns    table columns
     * @param ignoreNull ignore null value or not
     * @return named parameter insert statement
     */
    public String generateNamedParamInsert(final String tableName, Collection<String> columns, final Map<String, ?> data, boolean ignoreNull) {
        StringJoiner f = new StringJoiner(", ");
        StringJoiner h = new StringJoiner(", ");
        for (String column : columns) {
            if (ignoreNull && Objects.isNull(data.get(column))) {
                continue;
            }
            f.add(column);
            h.add(namedParamPrefix + column);
        }
        return "insert into " + tableName + "(" + f + ") values (" + h + ")";
    }

    /**
     * Generate named parameter update statement.
     *
     * @param tableName  table name
     * @param data       data
     * @param columns    table columns
     * @param ignoreNull ignore null value or not
     * @return named parameter update sets statement
     */
    public String generateNamedParamUpdate(String tableName, Collection<String> columns, Map<String, ?> data, boolean ignoreNull) {
        StringJoiner sb = new StringJoiner(",\n\t");
        for (String column : columns) {
            if (ignoreNull && Objects.isNull(data.get(column))) {
                continue;
            }
            sb.add(column + StandardOperator.EQ.padWithSpace() + namedParamPrefix + column);
        }
        return "update " + tableName + "\nset " + sb;
    }

    /**
     * Generate count query by record query.
     *
     * @param recordQuery record query
     * @return count query
     */
    public String generateCountQuery(@NotNull final String recordQuery) {
        return "select count(*) from (" + recordQuery + ") t_4_rabbit";
    }

    public Pattern getNamedParamPattern() {
        return namedParamPattern;
    }

    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }

    public NamedParamFormatter getNamedParamFormatter() {
        return namedParamFormatter;
    }

    public void setNamedParamFormatter(NamedParamFormatter namedParamFormatter) {
        this.namedParamFormatter = namedParamFormatter;
    }

    public TemplateFormatter getTemplateFormatter() {
        return templateFormatter;
    }

    public void setTemplateFormatter(TemplateFormatter templateFormatter) {
        this.templateFormatter = templateFormatter;
    }
}
