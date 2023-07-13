package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.StringFormatter;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.CollectionUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.CollectionUtil.*;
import static com.github.chengyuxing.sql.utils.SqlUtil.*;

/**
 * sql构建工具帮助类
 */
public class SqlGenerator {
    private static final Logger log = LoggerFactory.getLogger(SqlGenerator.class);
    private final String namedParamPrefix;
    private final StringFormatter stringFormatter;
    /**
     * 匹配命名参数
     */
    private final Pattern PARAM_PATTERN;

    /**
     * sql翻译帮助实例
     *
     * @param _namedParamPrefix 命名参数前缀符号
     */
    public SqlGenerator(char _namedParamPrefix) {
        if (_namedParamPrefix == ' ') {
            throw new IllegalArgumentException("prefix char cannot be empty.");
        }
        this.PARAM_PATTERN = Pattern.compile("(^\\" + _namedParamPrefix + "|[^\\" + _namedParamPrefix + "]\\" + _namedParamPrefix + ")(?<name>[a-zA-Z_][\\w_]*)", Pattern.MULTILINE);
        this.namedParamPrefix = String.valueOf(_namedParamPrefix);
        this.stringFormatter = new StringFormatter(_namedParamPrefix) {
            @Override
            protected String parseValue(Object value, boolean isSpecial) {
                return formatObject(value, isSpecial);
            }
        };
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
     * 获取字符串模版参数解析正则表达式
     *
     * @return 字符串模版参数解析正则表达式
     */
    public Pattern getSTR_TEMP_PATTERN() {
        return stringFormatter.getPattern();
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
     * 格式化sql字符串模版<br>
     * e.g.
     * <blockquote>
     * <pre>字符串：select ${ fields } from test.user where ${  cnd} and id in (${:idArr}) or id = ${:idArr.1}</pre>
     * <pre>参数：{fields: "id, name", cnd: "name = 'cyx'", idArr: ["a", "b", "c"]}</pre>
     * <pre>结果：select id, name from test.user where name = 'cyx' and id in ('a', 'b', 'c') or id = 'b'</pre>
     * </blockquote>
     *
     * @param template 带有字符串模版占位符的字符串
     * @param data     参数
     * @return 替换模版占位符后的字符串
     */
    public String formatSql(final String template, final Map<String, ?> data) {
        return stringFormatter.format(template, data);
    }

    /**
     * 构建一条可执行的sql
     *
     * @param sql     命名参数的sql字符串
     * @param args    参数
     * @param prepare 是否生成预编译sql
     * @return 预编译/普通sql和顺序的参数名集合
     */
    public Pair<String, List<String>> generateSql(final String sql, Map<String, ?> args, boolean prepare) {
        Map<String, ?> argx = args == null ? new HashMap<>() : args;
        // resolve the sql string template first
        String fullSql = formatSql(sql, argx);
        if (!fullSql.contains(namedParamPrefix)) {
            return Pair.of(fullSql, Collections.emptyList());
        }
        // exclude substr next
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(fullSql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Matcher matcher = PARAM_PATTERN.matcher(noneStrSql);
        List<String> names = new ArrayList<>();
        if (prepare) {
            while (matcher.find()) {
                String name = matcher.group("name");
                names.add(name);
                noneStrSql = StringUtil.replaceFirst(noneStrSql, namedParamPrefix + name, "?");
            }
        } else {
            while (matcher.find()) {
                String name = matcher.group("name");
                names.add(name);
                if (argx.containsKey(name)) {
                    String value = quoteFormatValue(argx.get(name));
                    noneStrSql = StringUtil.replaceFirst(noneStrSql, namedParamPrefix + name, value);
                } else if (containsKeyIgnoreCase(argx, name)) {
                    log.warn("cannot find name: '{}' in args: {}, auto get value by '{}' ignore case, maybe you should check your sql's named parameter and args.", name, argx, name);
                    String value = quoteFormatValue(getValueIgnoreCase(argx, name));
                    noneStrSql = StringUtil.replaceFirstIgnoreCase(noneStrSql, namedParamPrefix + name, value);
                }
            }
        }
        // finally, set placeholder into none-string-part sql
        for (Map.Entry<String, String> e : noneStrSqlAndHolder.getItem2().entrySet()) {
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
            if (StringUtil.equalsAnyIgnoreCase(k, fieldArr)) {
                if (!containsIgnoreCase(set, k)) {
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
            if (row.containsKey(key)) {
                f.add(key);
                v.add(quoteFormatValue(row.get(key)));
            }
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
            if (row.containsKey(key)) {
                f.add(key);
                h.add(namedParamPrefix + key);
            }
        }
        return "insert into " + tableName + "(" + f + ") \nvalues (" + h + ")";
    }

    /**
     * 构建一个传名参数占位符的更新语句
     *
     * @param tableName   表名
     * @param where       where条件
     * @param data        数据
     * @param tableFields 表字段集合
     * @return 传名参数占位符的更新语句
     */
    public String generateNamedParamUpdate(String tableName, String where, Map<String, ?> data, List<String> tableFields) {
        return generateUpdate(tableName, where, data, tableFields, true);
    }

    /**
     * 构建一个更新语句
     *
     * @param tableName    表名
     * @param where        where条件
     * @param data         数据
     * @param tableFields  表字段集合
     * @param isNamedParam 是否传名参数占位符
     * @return 传名参数占位符的sql或普通sql
     */
    public String generateUpdate(String tableName, String where, Map<String, ?> data, List<String> tableFields, boolean isNamedParam) {
        String whereStatement = where;
        Map<String, ?> updateSets = getUpdateSets(whereStatement, data);
        if (updateSets.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        Set<String> keys = filterKeys(updateSets, tableFields);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        StringJoiner sb = new StringJoiner(",\n\t");
        if (isNamedParam) {
            for (String key : keys) {
                if (updateSets.containsKey(key)) {
                    sb.add(key + " = " + namedParamPrefix + key);
                }
            }
        } else {
            for (String key : keys) {
                if (updateSets.containsKey(key)) {
                    String v = quoteFormatValue(updateSets.get(key));
                    sb.add(key + " = " + v);
                }
            }
            whereStatement = generateSql(whereStatement, data, false).getItem1();
        }
        return "update " + tableName + " \nset " + sb + "\nwhere " + whereStatement;
    }

    /**
     * 获取update语句中set需要的参数字典
     *
     * @param where 可包含参数的where条件
     * @param args  参数字典
     * @return set参数字典
     */
    public Map<String, ?> getUpdateSets(String where, Map<String, ?> args) {
        if (args == null || args.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, Object> sets = new HashMap<>();
        // 获取where条件中的参数名
        List<String> whereFields = getPreparedSql(where, args).getItem2();
        // 将where条件中的参数排除，因为where中的参数作为条件，而不是需要更新的值
        for (Map.Entry<String, ?> e : args.entrySet()) {
            if (!CollectionUtil.containsIgnoreCase(whereFields, e.getKey())) {
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

        }
        return "select count(*) from (" + recordQuery + ") r_data";
    }
}
