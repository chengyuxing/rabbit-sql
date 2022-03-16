package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Keywords;

import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.startsWithIgnoreCase;

/**
 * SQL工具类
 */
public class SqlUtil {
    /**
     * 匹配命名参数
     */
    public static final Pattern ARG_PATTERN = Pattern.compile("(^:|[^:]:)(?<name>[a-zA-Z_][\\w_]*)", Pattern.MULTILINE);
    /**
     * 匹配字符串单引号里面的内容
     */
    public static final Pattern SUB_STR_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);

    /**
     * 过滤筛选掉不满足条件的字段
     *
     * @param row    数据行
     * @param fields 需要包含的字段集合
     * @return 满足条件的字段
     */
    @SuppressWarnings("Java8CollectionRemoveIf")
    public static Set<String> filterKeys(final Map<String, ?> row, List<String> fields) {
        Set<String> keys = row.keySet();
        if (fields != null && !fields.isEmpty()) {
            Iterator<String> keyIterator = keys.iterator();
            while (keyIterator.hasNext()) {
                if (!fields.contains(keyIterator.next())) {
                    keyIterator.remove();
                }
            }
        }
        return keys;
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
    public static String generateInsert(final String tableName, final Map<String, ?> row, List<String> fields) {
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
     * 构建一个预编译的插入语句
     *
     * @param tableName 表名
     * @param row       数据
     * @param fields    需要包含的字段集合
     * @return 插入语句
     * @throws IllegalArgumentException 如果参数为空
     */
    public static String generatePreparedInsert(final String tableName, final Map<String, ?> row, List<String> fields) {
        Set<String> keys = filterKeys(row, fields);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate insert sql error.");
        }
        StringJoiner f = new StringJoiner(", ");
        StringJoiner h = new StringJoiner(", ");
        for (String key : keys) {
            f.add(key);
            h.add(":" + key);
        }
        return "insert into " + tableName + "(" + f + ") \nvalues (" + h + ")";
    }

    /**
     * 构建一个更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @param where     where条件
     * @return 更新语句
     * @throws IllegalArgumentException 如果where条件为空或者没有生成需要更新的字段
     */
    public static String generateUpdate(String tableName, Map<String, ?> data, final String where) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        if (where.trim().equals("")) {
            throw new IllegalArgumentException("where condition must not be empty.");
        }
        Pair<String, List<String>> sqlAndFields = generateSql(where, data, false);
        List<String> cndFields = sqlAndFields.getItem2();
        String w = sqlAndFields.getItem1();
        StringJoiner sb = new StringJoiner(",\n\t");
        for (String key : data.keySet()) {
            if (!key.startsWith("${") && !key.endsWith("}")) {
                String value = quoteFormatValueIfNecessary(data.get(key));
                if (!cndFields.contains(key)) {
                    sb.add(key + " = " + value);
                } else {
                    // 此处是条件中所包含的参数，放在where中，不在更新语句中
                    w = w.replace(":" + key, value);
                }
            }
        }
        String updateFields = sb.toString();
        if (!updateFields.equals("")) {
            w = startsWithIgnoreCase(w.trim(), "where") ? w : "where " + w;
            return "update " + tableName + " \nset " + sb + "\n" + w;
        }
        throw new IllegalArgumentException("generate error, there are no fields.");
    }

    /**
     * 构建一个预编译的更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @return 更新语句
     * @throws IllegalArgumentException 如果参数为空
     */
    public static String generatePreparedUpdate(String tableName, Map<String, ?> data) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        Set<String> keys = data.keySet();
        StringJoiner sb = new StringJoiner(",\n\t");
        for (String key : keys) {
            sb.add(key + " = :" + key);
        }
        return "update " + tableName + " \nset " + sb;
    }

    /**
     * 使用单引号包裹
     *
     * @param value 值
     * @return 使用引号包裹值
     */
    public static String quote(String value) {
        return "'" + value + "'";
    }

    /**
     * 使用单引号包裹，并安全的处理其中包含的引号
     *
     * @param value 值
     * @return 安全处理后的值
     */
    public static String safeQuote(String value) {
        if (value.contains("'")) {
            value = value.replace("'", "''");
        }
        return quote(value);
    }

    /**
     * 安全处理字符串引号和值类型进行默认格式化处理
     *
     * @param obj 值对象
     * @return 格式化后的值
     */
    public static String quoteFormatValueIfNecessary(Object obj) {
        if (obj == null) {
            return "null";
        }
        Class<?> clazz = obj.getClass();
        if (clazz == String.class) {
            return safeQuote((String) obj);
        }
        if (clazz == Integer.class ||
                clazz == Long.class ||
                clazz == Double.class ||
                clazz == Short.class ||
                clazz == Float.class ||
                clazz == Byte.class ||
                clazz == Boolean.class ||
                clazz.isPrimitive()) {
            return obj.toString();
        }
        if (clazz == Date.class) {
            return quote(((Date) obj).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
        }
        // I think you wanna save json string
        if (Map.class.isAssignableFrom(clazz) ||
                Collection.class.isAssignableFrom(clazz) ||
                !clazz.getTypeName().startsWith("java.")) {
            String value = ReflectUtil.obj2Json(obj);
            if (value != null) {
                return safeQuote(value);
            }
            return "null";
        }
        // default just quote
        return quote(obj.toString());
    }

    /**
     * 处理一段sql，将sql内出现的字符串（单引号包裹的部分）替换为一个特殊的占位符，并将替换的字符串存储下来，可对原sql进行一些其他处理操作，而不受到字符串内容的影响
     *
     * @param sql sql字符串
     * @return 替换字符串后带有特殊占位符的sql和占位符与字符串的映射
     */
    public static Pair<String, Map<String, String>> replaceSqlSubstr(final String sql) {
        String noneStrSql = sql;
        Map<String, String> mapper = new HashMap<>();
        Matcher m = SUB_STR_PATTERN.matcher(sql);
        while (m.find()) {
            // sql part of substr
            String str = m.group();
            // mapping placeholder
            String placeHolder = "\u02de" + (str.hashCode()) + "\u02de";
            noneStrSql = noneStrSql.replace(str, placeHolder);
            mapper.put(placeHolder, str);
        }
        return Pair.of(noneStrSql, mapper);
    }

    /**
     * 获取处理参数占位符预编译的SQL
     *
     * @param sql  带参数占位符的SQL
     * @param args 参数
     * @return 预编译SQL和顺序的参数名集合
     */
    public static Pair<String, List<String>> getPreparedSql(final String sql, Map<String, ?> args) {
        return generateSql(sql, args, true);
    }

    /**
     * 构建带有传名参数的sql
     *
     * @param sql     sql字符串
     * @param args    参数
     * @param prepare 是否生成预编译sql
     * @return 预编译/普通sql和顺序的参数名集合
     */
    public static Pair<String, List<String>> generateSql(final String sql, Map<String, ?> args, boolean prepare) {
        if (args.isEmpty()) {
            return Pair.of(sql, Collections.emptyList());
        }
        // exclude substr first
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(sql);
        // resolve the sql string template next
        String noneStrSql = resolveSqlStrTemplate(noneStrSqlAndHolder.getItem1(), args, false);
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        // maybe args contains substr.
        int x, y;
        while ((x = noneStrSql.indexOf("'")) >= 0 && (y = noneStrSql.lastIndexOf("'")) >= 0 && x != y) {
            Pair<String, Map<String, String>> againNoneStrSqlAndHolder = replaceSqlSubstr(noneStrSql);
            noneStrSql = resolveSqlStrTemplate(againNoneStrSqlAndHolder.getItem1(), args, false);
            placeholderMapper.putAll(againNoneStrSqlAndHolder.getItem2());
        }
        // safe to replace arg by name placeholder
        Matcher matcher = ARG_PATTERN.matcher(noneStrSql);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            String name = matcher.group("name");
            names.add(name);
            String value = prepare ? "?" : quoteFormatValueIfNecessary(args.get(name));
            noneStrSql = noneStrSql.replaceFirst(":" + name, value);
        }
        // finally, set placeholder into none-string-part sql
        for (String key : placeholderMapper.keySet()) {
            noneStrSql = noneStrSql.replace(key, placeholderMapper.get(key));
        }
        return Pair.of(noneStrSql, names);
    }

    /**
     * 排除sql字符串尾部的非sql语句部分的其他字符
     *
     * @param sql sql字符串
     * @return 去除分号后的sql
     */
    public static String trimEnd(String sql) {
        return sql.replaceAll("([\\s;]*)$", "");
    }

    /**
     * 解析字符串模版
     *
     * @param str          带有字符串模版占位符的字符串
     * @param args         参数
     * @param exceptSubstr 是否排除子字符串 --如果子字符串中包含字符串模版占位符，true：不解析，false：解析
     * @return 替换模版占位符后的字符串
     */
    @SuppressWarnings("unchecked")
    public static String resolveSqlStrTemplate2(final String str, final Map<String, ?> args, boolean exceptSubstr) {
        if (args == null || args.isEmpty()) {
            return str;
        }
        String noneStrSql = str;
        Map<String, String> substrMapper = null;
        if (exceptSubstr) {
            Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(str);
            noneStrSql = noneStrSqlAndHolder.getItem1();
            substrMapper = noneStrSqlAndHolder.getItem2();
        }
        if (!noneStrSql.contains("${")) {
            return str;
        }
        for (String key : args.keySet()) {
            if (key.startsWith("${") && key.endsWith("}")) {
                String trueKey = key;
                Object value = args.get(key);
                String subSql = "";
                if (value != null) {
                    Object[] values;
                    if (value instanceof Object[]) {
                        values = (Object[]) value;
                    } else if (value instanceof Collection) {
                        values = ((Collection<Object>) value).toArray();
                    } else {
                        values = new Object[]{value};
                    }
                    StringJoiner sb = new StringJoiner(", ");
                    if (key.startsWith("${:")) {
                        // expand and quote safe args
                        trueKey = "${" + key.substring(3);
                        for (Object v : values) {
                            sb.add(quoteFormatValueIfNecessary(v));
                        }
                    } else {
                        // just expand
                        for (Object v : values) {
                            if (v != null) {
                                sb.add(v.toString());
                            }
                        }
                    }
                    subSql = "\n" + sb.toString().trim() + "\n";
                }
                int partIndex;
                while ((partIndex = noneStrSql.indexOf(trueKey)) != -1) {
                    int start = StringUtil.searchIndexUntilNotBlank(noneStrSql, partIndex, true);
                    int end = StringUtil.searchIndexUntilNotBlank(noneStrSql, partIndex + trueKey.length() - 1, false);
                    noneStrSql = noneStrSql.substring(0, start + 1) + subSql + noneStrSql.substring(end);
                }
            }
        }
        if (exceptSubstr) {
            for (String key : substrMapper.keySet()) {
                noneStrSql = noneStrSql.replace(key, substrMapper.get(key));
            }
        }
        return noneStrSql;
    }

    @SuppressWarnings("unchecked")
    public static String resolveSqlStrTemplate(final String str, final Map<String, ?> args, boolean exceptSubstr) {
        if (args == null || args.isEmpty()) {
            return str;
        }
        String noneStrSql = str;
        Map<String, String> substrMapper = null;
        if (exceptSubstr) {
            Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(str);
            noneStrSql = noneStrSqlAndHolder.getItem1();
            substrMapper = noneStrSqlAndHolder.getItem2();
        }
        if (!noneStrSql.contains("${")) {
            return str;
        }
        for (String key : args.keySet()) {
            String tempKey = "${" + key + "}";
            String tempArrKey = "${:" + key + "}";
            if (StringUtil.containsAny(noneStrSql, tempKey, tempArrKey)) {
                String trueKey = tempKey;
                Object value = args.get(key);
                String subSql = "";
                if (value != null) {
                    Object[] values;
                    if (value instanceof Object[]) {
                        values = (Object[]) value;
                    } else if (value instanceof Collection) {
                        values = ((Collection<Object>) value).toArray();
                    } else {
                        values = new Object[]{value};
                    }
                    StringJoiner sb = new StringJoiner(", ");
                    if (noneStrSql.contains(tempArrKey)) {
                        // expand and quote safe args
                        trueKey = tempArrKey;
                        for (Object v : values) {
                            sb.add(quoteFormatValueIfNecessary(v));
                        }
                    } else {
                        // just expand
                        for (Object v : values) {
                            if (v != null) {
                                sb.add(v.toString());
                            }
                        }
                    }
                    subSql = "\n" + sb.toString().trim() + "\n";
                }
                int partIndex;
                while ((partIndex = noneStrSql.indexOf(trueKey)) != -1) {
                    int start = StringUtil.searchIndexUntilNotBlank(noneStrSql, partIndex, true);
                    int end = StringUtil.searchIndexUntilNotBlank(noneStrSql, partIndex + trueKey.length() - 1, false);
                    noneStrSql = noneStrSql.substring(0, start + 1) + subSql + noneStrSql.substring(end);
                }
            }
        }
        if (exceptSubstr) {
            for (String key : substrMapper.keySet()) {
                noneStrSql = noneStrSql.replace(key, substrMapper.get(key));
            }
        }
        return noneStrSql;
    }

    /**
     * 移除sql的块注释
     *
     * @param sql sql
     * @return 去除块注释的sql
     */
    public static String removeAnnotationBlock(final String sql) {
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(sql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        char[] chars = noneStrSql.toCharArray();
        List<Character> characters = new ArrayList<>();
        int count = 0;
        for (int i = 0; i < chars.length; i++) {
            int prev = i;
            int next = i;
            if (i > 0) {
                prev = i - 1;
            }
            if (i < chars.length - 1) {
                next = i + 1;
            }
            if (chars[i] == '/' && chars[next] == '*') {
                count++;
            } else if (chars[i] == '*' && chars[next] == '/') {
                count--;
            } else if (count == 0) {
                if (chars[prev] == '*') {
                    if (chars[i] != '/') {
                        characters.add(chars[i]);
                    }
                } else {
                    characters.add(chars[i]);
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        for (Character c : characters) {
            sb.append(c);
        }
        String noneBSql = sb.toString().replaceAll("\n\\s*\n", "\n");
        for (String key : placeholderMapper.keySet()) {
            noneBSql = noneBSql.replace(key, placeholderMapper.get(key));
        }
        return noneBSql;
    }

    /**
     * 获取sql的块注释
     *
     * @param sql sql
     * @return 块注释
     */
    public static List<String> getAnnotationBlock(final String sql) {
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(sql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        char[] chars = noneStrSql.toCharArray();
        StringBuilder annotations = new StringBuilder();
        int count = 0;
        for (int i = 0; i < chars.length; i++) {
            int prev = i;
            int next = i;
            if (i > 0) {
                prev = i - 1;
            }
            if (i < chars.length - 1) {
                next = i + 1;
            }
            if (chars[i] == '/' && chars[next] == '*') {
                count++;
                annotations.append("/");
            } else if (chars[i] == '*' && chars[next] == '/') {
                count--;
                annotations.append("*");
            } else if (count == 0) {
                if (chars[prev] == '*') {
                    if (chars[i] == '/') {
                        annotations.append("/").append("\u02ac");
                    }
                }
            } else {
                annotations.append(chars[i]);
            }
        }
        String annotationStr = annotations.toString();
        for (String key : placeholderMapper.keySet()) {
            if (annotationStr.contains(key)) {
                annotationStr = annotationStr.replace(key, placeholderMapper.get(key));
            }
        }
        return Arrays.asList(annotationStr.split("\u02ac"));
    }

    /**
     * 通过查询sql大致的构建出查询条数的sql
     *
     * @param recordQuery 查询sql
     * @return 条数查询sql
     */
    public static String generateCountQuery(final String recordQuery) {
        // for 0 errors, simple count query currently.
        return "select count(*) from (" + recordQuery + ") r_data";
    }

    /**
     * 处理sql字符串高亮
     *
     * @param sql sql字符串
     * @return 高亮sql
     */
    public static String highlightSql(String sql) {
        try {
            Pair<String, Map<String, String>> r = SqlUtil.replaceSqlSubstr(sql);
            String rSql = r.getItem1();
            Pair<List<String>, List<String>> x = StringUtil.regexSplit(rSql, "(?<sp>[\\s,\\[\\]()::;])", "sp");
            List<String> maybeKeywords = x.getItem1();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < maybeKeywords.size(); i++) {
                String key = maybeKeywords.get(i);
                if (!key.trim().equals("")) {
                    // keywords highlight
                    if (StringUtil.matchesAnyIgnoreCase(key, Keywords.STANDARD) || StringUtil.matchesAnyIgnoreCase(key, Keywords.POSTGRESQL)) {
                        maybeKeywords.set(i, Printer.colorful(key, Color.DARK_PURPLE));
                        // functions highlight
                    } else if (StringUtil.containsAnyIgnoreCase(key, Keywords.FUNCTIONS)) {
                        if (rSql.contains(key + "(")) {
                            maybeKeywords.set(i, Printer.colorful(key, Color.YELLOW));
                        }
                        // number highlight
                    } else if (StringUtil.isNumeric(key)) {
                        maybeKeywords.set(i, Printer.colorful(key, Color.DARK_CYAN));
                        // PostgreSQL function body block highlight
                    } else if (key.equals("$$")) {
                        maybeKeywords.set(i, Printer.colorful(key, Color.DARK_GREEN));
                        // symbol '*' highlight
                    } else if (key.contains("*") && !key.contains("/*") && !key.contains("*/")) {
                        maybeKeywords.set(i, key.replace("*", Printer.colorful("*", Color.YELLOW)));
                    }
                }
                sb.append(maybeKeywords.get(i));
                if (i < maybeKeywords.size() - 1) {
                    sb.append(x.getItem2().get(i));
                }
            }
            String colorfulSql = sb.toString();
            // reinsert the sub string
            Map<String, String> subStr = r.getItem2();
            for (String key : subStr.keySet()) {
                colorfulSql = colorfulSql.replace(key, Printer.colorful(subStr.get(key), Color.DARK_GREEN));
            }
            // resolve single annotation
            String[] sqlLine = colorfulSql.split("\n");
            for (int i = 0; i < sqlLine.length; i++) {
                String line = sqlLine[i];
                if (line.trim().startsWith("--")) {
                    sqlLine[i] = Printer.colorful(line.replaceAll("\033\\[\\d{2}m|\033\\[0m", ""), Color.SILVER);
                } else if (line.contains("--")) {
                    int idx = line.indexOf("--");
                    sqlLine[i] = line.substring(0, idx) + Printer.colorful(line.substring(idx).replaceAll("\033\\[\\d{2}m|\033\\[0m", ""), Color.SILVER);
                }
            }
            colorfulSql = String.join("\n", sqlLine);
            // resolve block annotation
            if (colorfulSql.contains("/*") && colorfulSql.contains("*/")) {
                List<String> annotations = getAnnotationBlock(colorfulSql);
                for (String annotation : annotations) {
                    colorfulSql = colorfulSql.replace(annotation, Printer.colorful(annotation.replaceAll("\033\\[\\d{2}m|\033\\[0m", ""), Color.SILVER));
                }
            }
            return colorfulSql;
        } catch (Exception e) {
            e.printStackTrace();
            return sql;
        }
    }
}
