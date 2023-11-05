package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.script.Patterns;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.StringUtil;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.CollectionUtil.containsIgnoreCase;
import static com.github.chengyuxing.sql.utils.SqlUtil.quoteFormatValue;
import static com.github.chengyuxing.sql.utils.SqlUtil.replaceSqlSubstr;

/**
 * Sql generate tool.
 */
public class SqlGenerator {
    private final String namedParamPrefix;
    /**
     * Named parameter pattern.
     */
    private final Pattern namedParamPattern;

    /**
     * Constructed a SqlGenerator with named parameter prefix.
     *
     * @param namedParamPrefix named parameter prefix
     */
    public SqlGenerator(char namedParamPrefix) {
        if (namedParamPrefix == ' ') {
            throw new IllegalArgumentException("prefix char cannot be empty.");
        }
        this.namedParamPattern = Pattern.compile("(^\\" + namedParamPrefix + "|[^\\" + namedParamPrefix + "]\\" + namedParamPrefix + ")(?<name>" + Patterns.VAR_KEY_PATTERN + ")");
        this.namedParamPrefix = String.valueOf(namedParamPrefix);
    }

    public Pattern getNamedParamPattern() {
        return namedParamPattern;
    }

    public String getNamedParamPrefix() {
        return namedParamPrefix;
    }

    /**
     * Generate prepared sql by named parameter sql.<br>
     *  e.g.
     * <blockquote>
     * <pre>before: select * from table where id = :id</pre>
     * <pre>after: select * from table where id = ?</pre>
     * </blockquote>
     *
     * @param sql  named parameter sql
     * @param args data of named parameter
     * @return [prepared sql, sorted arg names]
     */
    public Pair<String, List<String>> generatePreparedSql(final String sql, Map<String, ?> args) {
        return _generateSql(sql, args, true);
    }

    /**
     * Generate normal sql by named parameter sql.
     *
     * @param sql  named parameter sql
     * @param args data of named parameter
     * @return normal sql
     */
    public String generateSql(final String sql, Map<String, ?> args) {
        return _generateSql(sql, args, false).getItem1();
    }

    /**
     * Generate sql by named parameter sql.
     *
     * @param sql     named parameter sql
     * @param args    data of named parameter
     * @param prepare prepare or not
     * @return [prepare/normal sql，sorted arg names]
     */
    protected Pair<String, List<String>> _generateSql(final String sql, Map<String, ?> args, boolean prepare) {
        // resolve the sql string template first
        String fullSql = SqlUtil.formatSql(sql, args);
        if (!fullSql.contains(namedParamPrefix)) {
            return Triple.of(fullSql, Collections.emptyList(), Collections.emptyList());
        }
        // exclude substr next
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(fullSql);
        String noStrSql = noneStrSqlAndHolder.getItem1();
        Matcher matcher = namedParamPattern.matcher(noStrSql);
        List<String> names = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int pos = 0;
        while (matcher.find()) {
            int start = matcher.start("name");
            int end = matcher.end("name");
            String name = matcher.group("name");
            String replaced = "?";
            if (prepare) {
                names.add(name);
            } else {
                Object value = name.contains(".") ? ObjectUtil.getDeepValue(args, name) : args.get(name);
                replaced = quoteFormatValue(value);
            }
            sb.append(noStrSql, pos, start - 1).append(replaced);
            pos = end;
        }
        sb.append(noStrSql, pos, noStrSql.length());
        String result = sb.toString();
        for (Map.Entry<String, String> e : noneStrSqlAndHolder.getItem2().entrySet()) {
            result = result.replace(e.getKey(), e.getValue());
        }
        return Pair.of(result, names);
    }

    /**
     * Filter keys ignore case of data map if key not in custom keys scope.
     *
     * @param data       data map
     * @param keysScope keys scope
     * @return scoped key set
     */
    public Set<String> filterKeys(final Map<String, ?> data, List<String> keysScope) {
        if (keysScope == null || keysScope.isEmpty()) {
            return data.keySet();
        }
        String[] fieldArr = keysScope.toArray(new String[0]);
        Set<String> set = new HashSet<>();
        for (String k : data.keySet()) {
            if (StringUtil.equalsAnyIgnoreCase(k, fieldArr)) {
                if (!containsIgnoreCase(set, k)) {
                    set.add(k);
                }
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
        // 获取where条件中的参数名
        List<String> whereFields = generatePreparedSql(where, args).getItem2();
        // 将where条件中的参数排除，因为where中的参数作为条件，而不是需要更新的值
        // where id = :id
        for (Map.Entry<String, ?> e : args.entrySet()) {
            if (!containsIgnoreCase(whereFields, e.getKey())) {
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
}
