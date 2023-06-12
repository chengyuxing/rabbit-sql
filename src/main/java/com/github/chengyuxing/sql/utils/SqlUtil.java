package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Keywords;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.startsWithIgnoreCase;

/**
 * SQL工具类
 */
public class SqlUtil {
    public static final Pattern SUB_STR_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);

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
        if (obj instanceof String) {
            return safeQuote((String) obj);
        }
        if (ReflectUtil.isBasicType(obj)) {
            return obj.toString();
        }
        Class<?> clazz = obj.getClass();
        if (Date.class.isAssignableFrom(clazz)) {
            String dtStr = DateTimes.of(((Date) obj).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + "," + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == LocalDateTime.class) {
            String dtStr = DateTimes.of((LocalDateTime) obj).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + "," + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == LocalDate.class) {
            String dtStr = DateTimes.of((LocalDate) obj).toString("yyyy-MM-dd");
            return "to_date(" + quote(dtStr) + "," + quote("yyyy-mm-dd") + ")";
        }
        if (clazz == LocalTime.class) {
            String dtStr = DateTimes.of(((LocalTime) obj).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant()).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + "," + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == byte[].class) {
            return quote("blob:" + StringUtil.getSize((byte[]) obj));
        }
        if (Object[].class.isAssignableFrom(clazz)) {
            Object[] objArr = (Object[]) obj;
            String[] res = new String[objArr.length];
            for (int i = 0; i < res.length; i++) {
                Object o = objArr[i];
                if (o != null)
                    if (o instanceof String) {
                        res[i] = safeQuote(o.toString());
                    } else {
                        res[i] = o.toString();
                    }
            }
            return quote("{" + String.join(",", res) + "}");
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
        return safeQuote(obj.toString());
    }

    /**
     * 处理一段sql，将sql内出现的字符串（单引号包裹的部分）替换为一个特殊的占位符，并将替换的字符串存储下来，可对原sql进行一些其他处理操作，而不受到字符串内容的影响
     *
     * @param sql sql字符串
     * @return 替换字符串后带有特殊占位符的sql和占位符与字符串的映射
     */
    public static Pair<String, Map<String, String>> replaceSqlSubstr(final String sql) {
        if (!sql.contains("'")) {
            return Pair.of(sql, Collections.emptyMap());
        }
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
     * 排除sql字符串尾部的非sql语句部分的其他字符
     *
     * @param sql sql字符串
     * @return 去除分号后的sql
     */
    public static String trimEnd(String sql) {
        String tSql = sql.replaceAll("([\\s;]*)$", "");
        // oracle procedure syntax end with ';' is required.
        if (StringUtil.startsWithIgnoreCase(tSql, "begin") && StringUtil.endsWithIgnoreCase(tSql, "end")) {
            tSql += ";";
        }
        return tSql;
    }


    /**
     * 去除行注释，可能需要配合方法 {@link #replaceSqlSubstr(String)} 方法进行字符串安全处理
     *
     * @param line 行
     * @return 去除行注释的字符串
     */
    public static String cleanLineAnnotation(String line) {
        return line.substring(0, line.indexOf("--"));
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
     * 解构数组类型的值为适配sql的字符串
     *
     * @param value 可能是数组的值
     * @param quote 是否加引号
     * @return 匹配sql的字符串
     */
    @SuppressWarnings("unchecked")
    public static String deconstructArrayIfNecessary(Object value, boolean quote) {
        Object[] values;
        if (value instanceof Object[]) {
            values = (Object[]) value;
        } else if (value instanceof Collection) {
            values = ((Collection<Object>) value).toArray();
        } else {
            values = new Object[]{value};
        }
        StringJoiner sb = new StringJoiner(", ");
        if (quote) {
            // expand and quote safe args
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
        return sb.toString();
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
        // if update statement
        if (startsWithIgnoreCase(sql.trim(), "update")) {
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
                    if (StringUtil.equalsAnyIgnoreCase(key, Keywords.STANDARD) || StringUtil.equalsAnyIgnoreCase(key, Keywords.POSTGRESQL)) {
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

    /**
     * 创建适用于打印sql的字符串
     *
     * @param sql         sql字符串
     * @param isHighlight 是否语法高亮
     * @return 普通sql或语法高亮的sql
     */
    public static String buildPrintSql(String sql, boolean isHighlight) {
        if (isHighlight) {
            return highlightSql(sql);
        }
        return sql;
    }
}
