package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.script.Comparators;
import com.github.chengyuxing.common.script.impl.FastExpression;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Keywords;

import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.containsAllIgnoreCase;
import static com.github.chengyuxing.common.utils.StringUtil.startsWithIgnoreCase;

/**
 * SQL工具类
 */
public class SqlUtil {
    /**
     * 特殊字符用来防止字段名重复的问题
     */
    public static final String SEP = "\u02de";
    /**
     * 匹配命名参数
     */
    public static final Pattern ARG_PATTERN = Pattern.compile("(^:|[^:]:)(?<name>[\\w." + SEP + "]+)", Pattern.MULTILINE);
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
    public static Set<String> filterKeys(final Map<String, Object> row, List<String> fields) {
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
    public static String generateInsert(final String tableName, final Map<String, Object> row, List<String> fields) {
        Set<String> keys = filterKeys(row, fields);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate insert sql error.");
        }
        StringBuilder f = new StringBuilder();
        StringBuilder v = new StringBuilder();
        for (String key : keys) {
            f.append(key).append(", ");
            v.append(quoteFormatValueIfNecessary(row.get(key))).append(", ");
        }
        return "insert into " + tableName + "(" + f.substring(0, f.length() - 2) + ") \nvalues (" + v.substring(0, v.length() - 2) + ")";
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
    public static String generatePreparedInsert(final String tableName, final Map<String, Object> row, List<String> fields) {
        Set<String> keys = filterKeys(row, fields);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate insert sql error.");
        }
        StringBuilder f = new StringBuilder();
        StringBuilder h = new StringBuilder();
        for (String key : keys) {
            f.append(key).append(", ");
            h.append(":").append(key).append(", ");
        }
        return "insert into " + tableName + "(" + f.substring(0, f.length() - 2) + ") \nvalues (" + h.substring(0, h.length() - 2) + ")";
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
    public static String generateUpdate(String tableName, Map<String, Object> data, final String where) {
        if (where.trim().equals("")) {
            throw new IllegalArgumentException("where condition must not be empty.");
        }
        List<String> cndFields = getPreparedSql(where).getItem2();
        StringBuilder sb = new StringBuilder();
        String w = where;
        for (String key : data.keySet()) {
            String value = quoteFormatValueIfNecessary(data.get(key));
            if (!cndFields.contains(key)) {
                sb.append(key)
                        .append(" = ")
                        .append(value)
                        .append(",\n\t");
            } else {
                w = w.replace(":" + key, value);
            }
        }
        String updateFields = sb.toString();
        if (!updateFields.equals("")) {
            w = startsWithIgnoreCase(w.trim(), "where") ? w : "where " + w;
            return "update " + tableName + " \nset " + sb.substring(0, sb.lastIndexOf(",")) + "\n" + w;
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
    public static String generatePreparedUpdate(String tableName, Map<String, Object> data) {
        Set<String> keys = data.keySet();
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            if (!key.contains(SEP)) {
                sb.append(key).append(" = :").append(key).append(",\n\t");
            }
        }
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        return "update " + tableName + " \nset " + sb.substring(0, sb.lastIndexOf(","));
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
    public static Pair<String, Map<String, String>> replaceStrPartOfSql(final String sql) {
        String noneStrSql = sql;
        Map<String, String> mapper = new HashMap<>();
        Matcher m = SUB_STR_PATTERN.matcher(sql);
        int x = 0;
        while (m.find()) {
            // sql part of string
            String str = m.group();
            // indexed placeholder
            String placeHolder = SEP + (x++) + SEP;
            noneStrSql = noneStrSql.replace(str, placeHolder);
            mapper.put(placeHolder, str);
        }
        return Pair.of(noneStrSql, mapper);
    }

    /**
     * 获取处理参数占位符预编译的SQL
     *
     * @param sql 带参数占位符的SQL
     * @return 预编译SQL和参数名的集合
     */
    public static Pair<String, List<String>> getPreparedSql(final String sql) {
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceStrPartOfSql(sql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        // safe to replace arg by name placeholder
        Matcher matcher = ARG_PATTERN.matcher(noneStrSql);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            String name = matcher.group("name");
            names.add(name);
            noneStrSql = noneStrSql.replaceFirst(":" + name, "?");
        }
        // finally set placeholder into none-string-part sql
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
     * 解析SQL字符串
     *
     * @param sourceSql 原始sql字符串
     * @param args      参数
     * @return 替换模版占位符后的sql
     */
    @SuppressWarnings("unchecked")
    public static String resolveSqlPart(final String sourceSql, Map<String, Object> args) {
        if (args == null || args.size() == 0) {
            return sourceSql;
        }
        // exclude quote str look like '${name}', it's not placeholder.
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceStrPartOfSql(sourceSql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        if (!noneStrSql.contains("${")) {
            return sourceSql;
        }
        for (String key : args.keySet()) {
            if (key.startsWith("${") && key.endsWith("}")) {
                String trueKey = key;
                Object value = args.get(key);
                String subSql;
                if (key.startsWith("${...") || key.startsWith("${..:")) {
                    Object[] values;
                    if (value instanceof Object[]) {
                        values = (Object[]) value;
                    } else if (value instanceof Collection) {
                        values = ((Collection<Object>) value).toArray();
                    } else {
                        values = new Object[]{value};
                    }
                    StringBuilder sb = new StringBuilder();
                    // expand and quote safe args
                    if (key.startsWith("${..:")) {
                        trueKey = key.replace("${..:", "${");
                        for (Object v : values) {
                            sb.append(quoteFormatValueIfNecessary(v)).append(", ");
                        }
                    } else {
                        // just expand
                        trueKey = key.replace("${...", "${");
                        for (Object v : values) {
                            sb.append(v).append(", ");
                        }
                    }
                    subSql = sb.substring(0, sb.length() - 2);
                } else {
                    subSql = value.toString();
                }
                String start = subSql.startsWith("\n") ? "" : "\n";
                String v = start + subSql + "\n";
                noneStrSql = noneStrSql.replace(trueKey, v);
            }
        }
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        for (String key : placeholderMapper.keySet()) {
            noneStrSql = noneStrSql.replace(key, placeholderMapper.get(key));
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
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceStrPartOfSql(sql);
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
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceStrPartOfSql(sql);
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
     * 根据解析条件表达式的结果动态生成sql<br>
     * e.g. data.sql.template
     *
     * @param sql          sql
     * @param argsMap      参数字典
     * @param checkArgsKey 检查参数中是否存在表达式中需要计算的key
     * @return 解析后的sql
     * @see FastExpression
     */
    public static String dynamicSql(final String sql, Map<String, Object> argsMap, boolean checkArgsKey) {
        if (!containsAllIgnoreCase(sql, "--#if", "--#fi")) {
            return sql;
        }
        String nSql = removeAnnotationBlock(sql);
        String[] lines = nSql.split("\n");
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        boolean ok = true;
        boolean start = false;
        boolean inBlock = false;
        boolean blockFirstOk = false;
        boolean hasChooseBlock = containsAllIgnoreCase(sql, "--#choose", "--#end");
        for (String line : lines) {
            String trimLine = line.trim();
            if (!trimLine.isEmpty()) {
                if (first) {
                    if (!trimLine.startsWith("--")) {
                        first = false;
                    }
                }
                if (hasChooseBlock) {
                    if (startsWithIgnoreCase(trimLine, "--#choose")) {
                        blockFirstOk = false;
                        inBlock = true;
                        continue;
                    }
                    if (startsWithIgnoreCase(trimLine, "--#end")) {
                        inBlock = false;
                        continue;
                    }
                }
                if (startsWithIgnoreCase(trimLine, "--#if") && !start) {
                    start = true;
                    if (inBlock) {
                        if (!blockFirstOk) {
                            String filter = trimLine.substring(5);
                            FastExpression expression = FastExpression.of(filter);
                            expression.setCheckArgsKey(checkArgsKey);
                            ok = expression.calc(argsMap);
                            blockFirstOk = ok;
                        } else {
                            ok = false;
                        }
                    } else {
                        String filter = trimLine.substring(5);
                        FastExpression expression = FastExpression.of(filter);
                        expression.setCheckArgsKey(checkArgsKey);
                        ok = expression.calc(argsMap);
                    }
                    continue;
                }
                if (startsWithIgnoreCase(trimLine, "--#fi") && start) {
                    ok = true;
                    start = false;
                    continue;
                }
                if (ok) {
                    sb.append(line).append("\n");
                    if (!inBlock) {
                        blockFirstOk = false;
                    }
                }
            }
        }
        return repairSyntaxError(sb.toString());
    }

    /**
     * 修复sql常规语法错误<br>
     * e.g.
     * <blockquote>
     * <pre>where and/or/order/limit...</pre>
     * <pre>select ... from ...where</pre>
     * <pre>update ... set  a=b, where</pre>
     * </blockquote>
     *
     * @param sql sql语句
     * @return 修复后的sql
     */
    public static String repairSyntaxError(String sql) {
        Pattern p;
        Matcher m;
        String firstLine = sql.substring(0, sql.indexOf("\n")).trim();
        // if update statement
        if (startsWithIgnoreCase(firstLine, "update")) {
            p = Pattern.compile(",\\s*where", Pattern.CASE_INSENSITIVE);
            m = p.matcher(sql);
            if (m.find()) {
                sql = sql.substring(0, m.start()).concat(sql.substring(m.start() + 1));
            }
        }
        // "where and" statement
        p = Pattern.compile("where\\s+(and|or)\\s+", Pattern.CASE_INSENSITIVE);
        m = p.matcher(sql);
        if (m.find()) {
            return sql.substring(0, m.start() + 6).concat(sql.substring(m.end()));
        }
        // if "where order by ..." statement
        p = Pattern.compile("where\\s+(order by|limit|group by|union|\\))\\s+", Pattern.CASE_INSENSITIVE);
        m = p.matcher(sql);
        if (m.find()) {
            return sql.substring(0, m.start()).concat(sql.substring(m.start() + 6));
        }
        // if "where" at end
        p = Pattern.compile("where\\s*$", Pattern.CASE_INSENSITIVE);
        m = p.matcher(sql);
        if (m.find()) {
            return sql.substring(0, m.start());
        }
        return sql;
    }

    /**
     * 通过查询sql大致的构建出查询条数的sql
     *
     * @param recordQuery 查询sql
     * @return 条数查询sql
     */
    public static String generateCountQuery(final String recordQuery) {
        // for 0 errors, sorry!!!
        return "select count(*) from (" + recordQuery + ") _data";
    }

    /**
     * 处理sql字符串高亮
     *
     * @param sql sql字符串
     * @return 高亮sql
     */
    public static String highlightSql(String sql) {
        try {
            Pair<String, Map<String, String>> r = SqlUtil.replaceStrPartOfSql(sql);
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
                    } else if (key.matches(Comparators.NUMBER_REGEX)) {
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
