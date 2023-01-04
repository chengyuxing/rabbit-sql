package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.CollectionUtil.*;
import static com.github.chengyuxing.sql.utils.SqlUtil.quoteFormatValueIfNecessary;
import static com.github.chengyuxing.sql.utils.SqlUtil.replaceSqlSubstr;

/**
 * sql构建工具帮助类
 */
public class SqlTranslator {
    private static final Logger log = LoggerFactory.getLogger(SqlTranslator.class);
    private final String c;
    private static final char SPECIAL_CHAR = '\u0c32';
    /**
     * 匹配命名参数
     */
    Pattern PARAM_PATTERN = Pattern.compile("(^:|[^:]:)(?<name>[a-zA-Z_][\\w_]*)", Pattern.MULTILINE);

    Pattern STR_TEMP_PATTERN = Pattern.compile("\\$\\{\\s*(?<key>:?[\\w._-]+)\\s*}");

    /**
     * sql翻译帮助实例
     *
     * @param namedParameterPrefix 命名参数前缀符号
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
            STR_TEMP_PATTERN = Pattern.compile("\\$\\{\\s*(?<key>" + regC + "?[\\w._-]+)\\s*}");
        }
    }

    /**
     * 构建一条可执行的sql
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
        String fullSql = formatSql(sql, argx);
        // exclude substr next
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(fullSql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        Matcher matcher = PARAM_PATTERN.matcher(noneStrSql);
        List<String> names = new ArrayList<>();
        if (prepare) {
            while (matcher.find()) {
                String name = matcher.group("name");
                names.add(name);
                noneStrSql = StringUtil.replaceFirst(noneStrSql, c + name, "?");
            }
        } else {
            while (matcher.find()) {
                String name = matcher.group("name");
                names.add(name);
                if (argx.containsKey(name)) {
                    String value = quoteFormatValueIfNecessary(argx.get(name));
                    noneStrSql = StringUtil.replaceFirst(noneStrSql, c + name, value);
                } else if (containsKeyIgnoreCase(argx, name)) {
                    log.warn("cannot find name: '{}' in args: {}, auto get value by '{}' ignore case, maybe you should check your sql's named parameter and args.", name, args, name);
                    String value = quoteFormatValueIfNecessary(getValueIgnoreCase(argx, name));
                    noneStrSql = StringUtil.replaceFirstIgnoreCase(noneStrSql, c + name, value);
                }
            }
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
     * 格式化sql字符串模版<br>
     * e.g.
     * <blockquote>
     * <pre>字符串：select ${ fields } from test.user where ${  cnd} and id in (${:idArr}) or id = ${:idArr.1}</pre>
     * <pre>参数：{fields: "id, name", cnd: "name = 'cyx'", idArr: ["a", "b", "c"]}</pre>
     * <pre>结果：select id, name from test.user where name = 'cyx' and id in ("a", "b", "c") or id = "b"</pre>
     * </blockquote>
     *
     * @param str  带有字符串模版占位符的字符串
     * @param args 参数
     * @return 替换模版占位符后的字符串
     */
    public String formatSql(final String str, final Map<String, ?> args) {
        if (args == null || args.isEmpty()) {
            return str;
        }
        String copyStr = str;
        Matcher m = STR_TEMP_PATTERN.matcher(copyStr);
        if (m.find()) {
            // full str template key e.g. ${ :myKey  }
            String tempKey = m.group(0);
            // real key e.g. :myKey
            String key = m.group("key");
            boolean quote = key.startsWith(c);
            if (quote) {
                key = key.substring(1);
            }
            // e.g. user.cats.1
            // 字符串中有键路径，但参数中没有此路径键，才默认是键路径表达式，否分，默认args有名为此路径的键
            boolean isKeyPath = key.contains(".") && !args.containsKey(key);
            String tk = key;
            if (isKeyPath) {
                tk = key.substring(0, key.indexOf("."));
            }
            if (!args.containsKey(tk)) {
                copyStr = copyStr.replace(tempKey, SPECIAL_CHAR + tempKey.substring(1));
            }
            try {
                String subStr;
                if (isKeyPath) {
                    subStr = SqlUtil.deconstructArrayIfNecessary(ObjectUtil.getDeepNestValue(args, "/" + key.replace(".", "/")), quote);
                } else {
                    subStr = SqlUtil.deconstructArrayIfNecessary(args.get(key), quote);
                }
                copyStr = copyStr.replace(tempKey, subStr);
                return formatSql(copyStr, args);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        if (copyStr.lastIndexOf(SPECIAL_CHAR) != -1) {
            copyStr = copyStr.replace(SPECIAL_CHAR, '$');
        }
        return copyStr;
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
                v.add(quoteFormatValueIfNecessary(row.get(key)));
            } else if (containsKeyIgnoreCase(row, key)) {
                log.warn("cannot find name: '{}' in args: {}, auto get value by '{}' ignore case, maybe you should check your sql's named parameter and args.", key, row, key);
                f.add(key);
                v.add(quoteFormatValueIfNecessary(getValueIgnoreCase(row, key)));
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
            if (row.containsKey(key) || containsKeyIgnoreCase(row, key)) {
                f.add(key);
                h.add(c + key);
            }
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
        if (isNamedParam) {
            for (String key : keys) {
                if (data.containsKey(key) || containsKeyIgnoreCase(data, key)) {
                    sb.add(key + " = " + c + key);
                }
            }
        } else {
            for (String key : keys) {
                if (data.containsKey(key)) {
                    String v = quoteFormatValueIfNecessary(data.get(key));
                    sb.add(key + " = " + v);
                } else if (containsKeyIgnoreCase(data, key)) {
                    log.warn("cannot find name: '{}' in args: {}, auto get value by '{}' ignore case, maybe you should check your sql's named parameter and args.", key, data, key);
                    String v = quoteFormatValueIfNecessary(getValueIgnoreCase(data, key));
                    sb.add(key + " = " + v);
                }
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
        if (StringUtil.startsWithIgnoreCase(recordQuery.trim(), "with")) {

        }
        return "select count(*) from (" + recordQuery + ") r_data";
    }
}
