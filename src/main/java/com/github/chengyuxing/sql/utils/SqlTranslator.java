package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.CollectionUtil;
import com.github.chengyuxing.common.utils.StringUtil;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.sql.utils.SqlUtil.quoteFormatValueIfNecessary;
import static com.github.chengyuxing.sql.utils.SqlUtil.replaceSqlSubstr;

/**
 * sql构建工具帮助类
 */
public class SqlTranslator {
    private final String c;
    /**
     * 匹配命名参数
     */
    Pattern PARAM_PATTERN = Pattern.compile("(^:|[^:]:)(?<name>[a-zA-Z_][\\w_]*)", Pattern.MULTILINE);

    /**
     * sql翻译帮助实例
     *
     * @param namedParameterPrefix 命名参数前缀符号，默认为 ':' 冒号
     */
    public SqlTranslator(char namedParameterPrefix) {
        if (namedParameterPrefix == ' ') {
            throw new IllegalArgumentException("prefix char cannot be empty.");
        }
        String cs = String.valueOf(namedParameterPrefix);
        if (cs.equals(":")) {
            this.c = ":";
        } else {
            this.c = cs;
            String regC = cs.replace(cs, "\\" + cs);
            PARAM_PATTERN = Pattern.compile("(^" + regC + "|[^" + regC + "]" + regC + ")(?<name>[a-zA-Z_][\\w_]*)", Pattern.MULTILINE);
        }
    }

    /**
     * 构建带有传名参数的sql
     *
     * @param sql     sql字符串
     * @param args    参数
     * @param prepare 是否生成预编译sql
     * @return 预编译/普通sql和顺序的参数名集合
     */
    public Pair<String, List<String>> generateSql(final String sql, Map<String, ?> args, boolean prepare) {
        Map<String, ?> argx = new HashMap<>();
        if (args != null) {
            argx = new HashMap<>(args);
        }
        // resolve the sql string template first
        String fullSql = resolveSqlStrTemplate(sql, argx);
        // exclude substr next
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(fullSql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        Matcher matcher = PARAM_PATTERN.matcher(noneStrSql);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            String name = matcher.group("name");
            names.add(name);
            String value = prepare ? "?" : quoteFormatValueIfNecessary(argx.get(name));
            noneStrSql = noneStrSql.replace(c + name, value);
        }
        // finally, set placeholder into none-string-part sql
        for (Map.Entry<String, String> e : placeholderMapper.entrySet()) {
            noneStrSql = noneStrSql.replace(e.getKey(), e.getValue());
        }
        return Pair.of(noneStrSql, names);
    }

    /**
     * 解析传名参数sql处理为预编译的sql
     *
     * @param sql  带参数占位符的sql
     * @param args 参数
     * @return 预编译SQL和顺序的参数名集合
     */
    public Pair<String, List<String>> getPreparedSql(final String sql, Map<String, ?> args) {
        return generateSql(sql, args, true);
    }

    /**
     * 解析字符串模版，子字符串嵌套深度最大为8层
     *
     * @param str  带有字符串模版占位符的字符串
     * @param args 参数
     * @return 替换模版占位符后的字符串
     */
    public String resolveSqlStrTemplate(final String str, final Map<String, ?> args) {
        return resolveSqlStrTemplateRec(str, args, 8);
    }

    /**
     * 解析字符串模版
     *
     * @param str  带有字符串模版占位符的字符串
     * @param args 参数
     * @param deep 子字符串嵌套最大深度
     * @return 替换模版占位符后的字符串
     */
    public String resolveSqlStrTemplateRec(final String str, final Map<String, ?> args, int deep) {
        if (args == null || args.isEmpty()) {
            return str;
        }
        String sql = str;
        if (!sql.contains("${") || deep == 0) {
            return str;
        }
        for (Map.Entry<String, ?> e : args.entrySet()) {
            String tempKey = "${" + e.getKey() + "}";
            String tempArrKey = "${" + c + e.getKey() + "}";
            if (StringUtil.containsAny(sql, tempKey, tempArrKey)) {
                String trueKey = tempKey;
                Object value = e.getValue();
                String subSql = "";
                if (value != null) {
                    boolean quote = sql.contains(tempArrKey);
                    if (quote) {
                        trueKey = tempArrKey;
                    }
                    subSql = SqlUtil.deconstructArrayIfNecessary(value, quote).trim();
                }
                sql = sql.replace(trueKey, subSql);
            }
        }
        if (sql.contains("${") && sql.contains("}")) {
            return resolveSqlStrTemplateRec(sql, args, --deep);
        }
        return sql;
    }

    /**
     * 忽略大小写过滤筛选掉不满足条件的字段
     *
     * @param row    数据行
     * @param fields 需要包含的字段集合
     * @return 满足条件的字段
     */
    public Set<String> filterKeys(final Map<String, ?> row, List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return row.keySet();
        }
        String[] fieldArr = fields.toArray(new String[0]);
        Set<String> set = new HashSet<>();
        for (String k : row.keySet()) {
            if (StringUtil.containsAnyIgnoreCase(k, fieldArr)) {
                if (!CollectionUtil.containsIgnoreCase(set, k)) {
                    set.add(k);
                }
            }
        }
        return set;
    }

    /**
     * 构建一个普通插入语句
     *
     * @param tableName 表名
     * @param row       数据
     * @param fields    需要包含的字段集合
     * @return 插入语句
     * @throws IllegalArgumentException 如果参数为空
     */
    public String generateInsert(final String tableName, final Map<String, ?> row, List<String> fields) {
        Set<String> keys = filterKeys(row, fields);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate insert sql error.");
        }
        StringJoiner f = new StringJoiner(", ");
        StringJoiner v = new StringJoiner(", ");
        for (String key : keys) {
            f.add(key);
            v.add(quoteFormatValueIfNecessary(row.get(key)));
        }
        return "insert into " + tableName + "(" + f + ") \nvalues (" + v + ")";
    }

    /**
     * 构建一个传名参数占位符的插入语句
     *
     * @param tableName 表名
     * @param row       数据
     * @param fields    需要包含的字段集合
     * @return 传名参数占位符的插入语句
     * @throws IllegalArgumentException 如果参数为空
     */
    public String generateNamedParamInsert(final String tableName, final Map<String, ?> row, List<String> fields) {
        Set<String> keys = filterKeys(row, fields);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate insert sql error.");
        }
        StringJoiner f = new StringJoiner(", ");
        StringJoiner h = new StringJoiner(", ");
        for (String key : keys) {
            f.add(key);
            h.add(c + key);
        }
        return "insert into " + tableName + "(" + f + ") \nvalues (" + h + ")";
    }

    /**
     * 构建一个普通更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @param fields    需要包含的字段集合
     * @return 更新语句
     */
    public String generateUpdate(String tableName, Map<String, ?> data, List<String> fields) {
        return generateUpdate(tableName, data, fields, false);
    }

    /**
     * 构建一个普通更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @return 更新语句
     * @throws IllegalArgumentException 如果where条件为空或者没有生成需要更新的字段
     */
    public String generateUpdate(String tableName, Map<String, ?> data) {
        return generateUpdate(tableName, data, Collections.emptyList());
    }

    /**
     * 构建一个传名参数占位符的更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @param fields    需要包含的字段集合
     * @return 传名参数占位符的更新语句
     */
    public String generateNamedParamUpdate(String tableName, Map<String, ?> data, List<String> fields) {
        return generateUpdate(tableName, data, fields, true);
    }

    /**
     * 构建一个传名参数的更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @return 传名参数的更新语句
     * @throws IllegalArgumentException 如果参数为空
     */
    public String generateNamedParamUpdate(String tableName, Map<String, ?> data) {
        return generateNamedParamUpdate(tableName, data, Collections.emptyList());
    }

    /**
     * 构建一个更新语句
     *
     * @param tableName    表名
     * @param data         数据
     * @param fields       需要包含的字段集合
     * @param isNamedParam 是否传名参数占位符
     * @return 传名参数占位符的sql或普通sql
     */
    public String generateUpdate(String tableName, Map<String, ?> data, List<String> fields, boolean isNamedParam) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        Set<String> keys = filterKeys(data, fields);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        StringJoiner sb = new StringJoiner(",\n\t");
        for (String key : keys) {
            if (!key.startsWith("${") && !key.endsWith("}")) {
                String v = isNamedParam ? c + key : quoteFormatValueIfNecessary(data.get(key));
                sb.add(key + " = " + v);
            }
        }
        return "update " + tableName + " \nset " + sb;
    }

    /**
     * 通过查询sql大致的构建出查询条数的sql
     *
     * @param recordQuery 查询sql
     * @return 条数查询sql
     */
    public String generateCountQuery(final String recordQuery) {
        // for 0 errors, simple count query currently.
        return "select count(*) from (" + recordQuery + ") r_data";
    }
}
