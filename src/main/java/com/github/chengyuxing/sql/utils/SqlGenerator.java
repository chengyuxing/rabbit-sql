package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.script.expression.Patterns;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.support.NamedParamFormatter;
import com.github.chengyuxing.sql.support.TemplateFormatter;

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
     * Non-prepared Sql named parameter ({@code :key}) value formatter.
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
        private final String namedParamSql;
        private final String resultSql;
        private final Map<String, List<Integer>> argNameIndexMapping;
        private final Map<String, ?> args;

        /**
         * Construct a new GeneratedSqlMetaData instance.
         *
         * @param namedParamSql       named parameter sql
         * @param resultSql           prepared sql or normal sql
         * @param argNameIndexMapping prepared sql arg name index mapping
         * @param args                args
         */
        public GeneratedSqlMetaData(String namedParamSql, String resultSql, Map<String, List<Integer>> argNameIndexMapping, Map<String, ?> args) {
            this.namedParamSql = namedParamSql;
            this.resultSql = resultSql;
            this.argNameIndexMapping = argNameIndexMapping;
            this.args = args;
        }

        public String getNamedParamSql() {
            return namedParamSql;
        }

        public String getResultSql() {
            return resultSql;
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
            if (!(o instanceof GeneratedSqlMetaData)) return false;

            GeneratedSqlMetaData that = (GeneratedSqlMetaData) o;
            return Objects.equals(getNamedParamSql(), that.getNamedParamSql()) && Objects.equals(getResultSql(), that.getResultSql()) && Objects.equals(getArgNameIndexMapping(), that.getArgNameIndexMapping()) && Objects.equals(getArgs(), that.getArgs());
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(getNamedParamSql());
            result = 31 * result + Objects.hashCode(getResultSql());
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
    public GeneratedSqlMetaData generatePreparedSql(final String sql, Map<String, ?> args) {
        return parseNamedParameterSql(sql, args, true);
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
        return parseNamedParameterSql(sql, args, false).getResultSql();
    }

    /**
     * Generate sql by named parameter sql.
     *
     * @param sql     named parameter sql
     * @param args    data of named parameter
     * @param prepare prepare or not
     * @return GeneratedSqlMetaData
     */
    protected GeneratedSqlMetaData parseNamedParameterSql(final String sql, Map<String, ?> args, boolean prepare) {
        // resolve the sql string template first
        String fullSql = SqlUtil.formatSql(sql, args, templateFormatter);
        if (fullSql.lastIndexOf(namedParamPrefix) < 0) {
            return new GeneratedSqlMetaData(sql, fullSql, Collections.emptyMap(), args);
        }
        Map<String, List<Integer>> indexMap = new HashMap<>();
        StringBuilder parsedSql = new StringBuilder();
        Matcher matcher = namedParamPattern.matcher(fullSql);
        int index = 1;
        int lastMatchEnd = 0;
        while (matcher.find()) {
            parsedSql.append(fullSql, lastMatchEnd, matcher.start());
            String name = matcher.group(1);
            if (name != null) {
                if (prepare) {
                    if (!indexMap.containsKey(name)) {
                        indexMap.put(name, new ArrayList<>());
                    }
                    indexMap.get(name).add(index);
                    parsedSql.append("?");
                    index++;
                } else {
                    Object value = name.contains(".") ? ObjectUtil.getDeepValue(args, name) : args.get(name);
                    parsedSql.append(namedParamFormatter.format(value));
                }
            } else {
                parsedSql.append(matcher.group());
            }
            lastMatchEnd = matcher.end();
        }
        parsedSql.append(fullSql.substring(lastMatchEnd));
        return new GeneratedSqlMetaData(sql, parsedSql.toString(), indexMap, args);
    }

    /**
     * Filter keys ignore case of data map if key not in custom keys scope.
     *
     * @param data      data map
     * @param keysScope keys scope
     * @return scoped key set
     */
    public Set<String> filterKeys(final Map<String, ?> data, List<String> keysScope) {
        if (keysScope == null || keysScope.isEmpty()) {
            return data.keySet();
        }
        String[] keysScopeArray = keysScope.toArray(new String[0]);
        Set<String> set = new HashSet<>();
        for (String k : data.keySet()) {
            if (StringUtil.equalsAnyIgnoreCase(k, keysScopeArray)) {
                set.add(k);
            }
        }
        return set;
    }

    /**
     * Generate named parameter insert statement.
     *
     * @param tableName  table name
     * @param data       data
     * @param keysScope  keys scope
     * @param ignoreNull ignore null value or not
     * @return named parameter insert statement
     * @throws IllegalArgumentException if all data keys not in scope
     */
    public String generateNamedParamInsert(final String tableName, final Map<String, ?> data, List<String> keysScope, boolean ignoreNull) {
        Set<String> keys = filterKeys(data, keysScope);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty key set, generate insert sql error.");
        }
        StringJoiner f = new StringJoiner(", ");
        StringJoiner h = new StringJoiner(", ");
        for (String key : keys) {
            if (data.containsKey(key)) {
                if (ignoreNull && Objects.isNull(data.get(key))) {
                    continue;
                }
                f.add(key);
                h.add(namedParamPrefix + key);
            }
        }
        return "insert into " + tableName + "(" + f + ") values (" + h + ")";
    }

    /**
     * Generate named parameter update statement.
     *
     * @param tableName  table name
     * @param where      condition
     * @param data       data
     * @param keysScope  keys scope
     * @param ignoreNull ignore null value or not
     * @return named parameter update statement
     */
    public String generateNamedParamUpdate(String tableName, String where, Map<String, ?> data, List<String> keysScope, boolean ignoreNull) {
        Map<String, ?> updateSets = getUpdateSets(where, data);
        if (updateSets.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        Set<String> keys = filterKeys(updateSets, keysScope);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        StringJoiner sb = new StringJoiner(",\n\t");
        for (String key : keys) {
            if (updateSets.containsKey(key)) {
                if (ignoreNull && Objects.isNull(updateSets.get(key))) {
                    continue;
                }
                sb.add(key + " = " + namedParamPrefix + key);
            }
        }
        return "update " + tableName + "\nset " + sb + "\nwhere " + where;
    }

    /**
     * Get update statement sets data from data map exclude keys in condition.
     *
     * @param where condition
     * @param args  args
     * @return update sets data
     */
    public Map<String, ?> getUpdateSets(String where, Map<String, ?> args) {
        if (Objects.isNull(args) || args.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, Object> sets = new HashMap<>();
        // pick out named parameter from where condition.
        Set<String> whereFields = generatePreparedSql(where, args).getArgNameIndexMapping().keySet();
        // for build correct update sets excludes the arg which in where condition.
        // where id = :id
        for (Map.Entry<String, ?> e : args.entrySet()) {
            if (!whereFields.contains(e.getKey())) {
                sets.put(e.getKey(), e.getValue());
            }
        }
        return sets;
    }

    /**
     * Generate count query by record query.
     *
     * @param recordQuery record query
     * @return count query
     */
    public String generateCountQuery(final String recordQuery) {
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
