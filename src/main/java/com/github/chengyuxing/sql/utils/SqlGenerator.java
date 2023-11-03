package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.script.Patterns;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.StringUtil;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.CollectionUtil.containsIgnoreCase;
import static com.github.chengyuxing.sql.utils.SqlUtil.quoteFormatValue;
import static com.github.chengyuxing.sql.utils.SqlUtil.replaceSqlSubstr;

/**
 * sql构建工具帮助类
 */
public class SqlGenerator {
    private final String namedParamPrefix;
    /**
     * 匹配命名参数
     */
    private final Pattern PARAM_PATTERN;

    /**
     * sql翻译帮助实例
     *
     * @param namedParamPrefix 命名参数前缀符号
     */
    public SqlGenerator(char namedParamPrefix) {
        if (namedParamPrefix == ' ') {
            throw new IllegalArgumentException("prefix char cannot be empty.");
        }
        this.PARAM_PATTERN = Pattern.compile("(^\\" + namedParamPrefix + "|[^\\" + namedParamPrefix + "]\\" + namedParamPrefix + ")(?<name>" + Patterns.VAR_KEY_PATTERN + ")");
        this.namedParamPrefix = String.valueOf(namedParamPrefix);
    }

    /**
     * 获取命名参数解析正则表达式
     *
     * @return 命名参数解析正则表达式
     */
    public Pattern getPARAM_PATTERN() {
        return PARAM_PATTERN;
    }

    /**
     * 获取命名参数前缀符号
     *
     * @return 命名参数前缀符号
     */
    public String getNamedParamPrefix() {
        return namedParamPrefix;
    }

    /**
     * 解析命名参数sql处理为预编译的sql
     *
     * @param sql  命名参数占位符sql
     * @param args 参数
     * @return 预编译SQL和顺序的参数名集合
     */
    public Pair<String, List<String>> generatePreparedSql(final String sql, Map<String, ?> args) {
        return _generateSql(sql, args, true);
    }

    /**
     * 解析命名参数sql处理为普通sql
     *
     * @param sql  命名参数占位符sql
     * @param args 参数
     * @return 普通sql
     */
    public String generateSql(final String sql, Map<String, ?> args) {
        return _generateSql(sql, args, false).getItem1();
    }

    /**
     * 构建一条可执行的sql
     *
     * @param sql     命名参数的sql字符串
     * @param args    参数
     * @param prepare 是否生成预编译sql
     * @return 预编译/普通sql和顺序的参数名集合
     */
    protected Pair<String, List<String>> _generateSql(final String sql, Map<String, ?> args, boolean prepare) {
        Map<String, ?> data = args == null ? new HashMap<>() : args;
        // resolve the sql string template first
        String fullSql = SqlUtil.formatSql(sql, data);
        if (!fullSql.contains(namedParamPrefix)) {
            return Pair.of(fullSql, Collections.emptyList());
        }
        // exclude substr next
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(fullSql);
        String noStrSql = noneStrSqlAndHolder.getItem1();
        Matcher matcher = PARAM_PATTERN.matcher(noStrSql);
        List<String> names = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int pos = 0;
        while (matcher.find()) {
            int start = matcher.start("name");
            int end = matcher.end("name");
            String name = matcher.group("name");
            String value = "?";
            if (!prepare) {
                if (name.contains(".")) {
                    value = quoteFormatValue(ObjectUtil.getDeepValue(data, name));
                } else {
                    value = quoteFormatValue(data.get(name));
                }
            }
            names.add(name);
            sb.append(noStrSql, pos, start - 1).append(value);
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
     * 忽略大小写过滤筛选掉不满足条件的字段
     *
     * @param row         数据行
     * @param fieldsScope 只允许数据行key存在的字段范围
     * @return 满足条件的字段
     */
    public Set<String> filterKeys(final Map<String, ?> row, List<String> fieldsScope) {
        if (fieldsScope == null || fieldsScope.isEmpty()) {
            return row.keySet();
        }
        String[] fieldArr = fieldsScope.toArray(new String[0]);
        Set<String> set = new HashSet<>();
        for (String k : row.keySet()) {
            if (StringUtil.equalsAnyIgnoreCase(k, fieldArr)) {
                if (!containsIgnoreCase(set, k)) {
                    set.add(k);
                }
            }
        }
        return set;
    }

    /**
     * 构建一个传名参数占位符的插入语句
     *
     * @param tableName   表名
     * @param row         数据
     * @param fieldsScope 只允许数据行key存在的字段范围
     * @param ignoreNull  忽略null值
     * @return 传名参数占位符的插入语句
     * @throws IllegalArgumentException 如果参数为空
     */
    public String generateNamedParamInsert(final String tableName, final Map<String, ?> row, List<String> fieldsScope, boolean ignoreNull) {
        Set<String> keys = filterKeys(row, fieldsScope);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate insert sql error.");
        }
        StringJoiner f = new StringJoiner(", ");
        StringJoiner h = new StringJoiner(", ");
        for (String key : keys) {
            if (row.containsKey(key)) {
                if (ignoreNull && Objects.isNull(row.get(key))) {
                    continue;
                }
                f.add(key);
                h.add(namedParamPrefix + key);
            }
        }
        return "insert into " + tableName + "(" + f + ") values (" + h + ")";
    }

    /**
     * 构建一个更新语句
     *
     * @param tableName   表名
     * @param where       where条件
     * @param data        数据
     * @param fieldsScope 只允许数据行key存在的字段范围
     * @param ignoreNull  忽略null值
     * @return 传名参数占位符的sql或普通sql
     */
    public String generateNamedParamUpdate(String tableName, String where, Map<String, ?> data, List<String> fieldsScope, boolean ignoreNull) {
        Map<String, ?> updateSets = getUpdateSets(where, data);
        if (updateSets.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        Set<String> keys = filterKeys(updateSets, fieldsScope);
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
     * 获取update语句中set需要的参数字典
     *
     * @param where 可包含参数的where条件
     * @param args  参数字典
     * @return set参数字典
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
     * 通过查询sql大致的构建出查询条数的sql
     *
     * @param recordQuery 查询sql
     * @return 条数查询sql
     */
    public String generateCountQuery(final String recordQuery) {
        if (StringUtil.startsWithIgnoreCase(recordQuery.trim(), "with")) {
            // TODO: 2023/10/25 考虑是否需要特殊处理下with查询的count语句
        }
        return "select count(*) from (" + recordQuery + ") r_data";
    }
}
