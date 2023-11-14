package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.common.StringFormatter;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.Jackson;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Keywords;
import com.github.chengyuxing.sql.types.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL util.
 */
public class SqlUtil {
    private static final Logger log = LoggerFactory.getLogger(SqlUtil.class);
    //language=RegExp
    public static final Pattern STR_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);
    //language=RegExp
    public static final Pattern SQL_ERR_COMMA_WHERE = Pattern.compile(",\\s*where", Pattern.CASE_INSENSITIVE);
    //language=RegExp
    public static final Pattern SQL_ERR_WHERE_AND_OR = Pattern.compile("where\\s+(and|or)\\s+", Pattern.CASE_INSENSITIVE);
    //language=RegExp
    public static final Pattern SQL_ERR_WHERE_ORDER = Pattern.compile("where(\\s+order|\\s+limit|\\s+group|\\s+union|\\s*\\))\\s+", Pattern.CASE_INSENSITIVE);
    //language=RegExp
    public static final Pattern SQL_ERR_WHERE_END = Pattern.compile("where\\s*$", Pattern.CASE_INSENSITIVE);
    @SuppressWarnings("UnnecessaryUnicodeEscape")
    public static final String SYMBOL = "\u02de";

    public static final StringFormatter FMT = new StringFormatter() {
        @Override
        protected String parseValue(Object value, boolean isSpecial) {
            return SqlUtil.parseValue(value, isSpecial);
        }
    };

    /**
     * Format sql string.<br>
     * e.g.
     * <blockquote>
     * <pre>sql：select ${ fields } from test.user where ${  cnd} and id in (${!idArr}) or id = ${!idArr.1} and dt <= ${!now}</pre>
     * <pre>args：{fields: "id, name", cnd: "name = 'cyx'", idArr: ["a", "b", "c"], now: {@link LocalDateTime#now() LocalDateTime.now()}}</pre>
     * <pre>result：select id, name from test.user where name = 'cyx' and id in ('a', 'b', 'c') or id = 'b'
     *       and dt <= to_timestamp('2023-11-14 19:14:34', 'yyyy-mm-dd hh24:mi:ss')</pre>
     * </blockquote>
     * Notice: If {@link Variable} type detected, {@link Variable#stringLiteral() stringLiteral()} method will be invoked.
     *
     * @param template sql string with template variable
     * @param data     data
     * @return formatted sql string
     * @see Variable
     */
    public static String formatSql(final String template, final Map<String, ?> data) {
        return FMT.format(template, data);
    }

    /**
     * String value with single quotes.
     *
     * @param value string value
     * @return string value with single quotes
     */
    public static String quote(String value) {
        return "'" + value + "'";
    }

    /**
     * String value with single safe quotes.
     *
     * @param value string value
     * @return string value with single safe quotes
     */
    public static String safeQuote(String value) {
        if (value.contains("'")) {
            value = value.replace("'", "''");
        }
        return quote(value);
    }

    /**
     * Object value safe format to string literal value.
     *
     * @param obj object value
     * @return formatted string literal value
     */
    public static String quoteFormatValue(Object obj) {
        if (obj == null) {
            return "null";
        }
        if (obj instanceof String) {
            return safeQuote((String) obj);
        }
        if (obj instanceof Character) {
            return safeQuote(obj.toString());
        }
        if (ReflectUtil.isBasicType(obj)) {
            return obj.toString();
        }
        Class<?> clazz = obj.getClass();
        if (Date.class.isAssignableFrom(clazz)) {
            String dtStr = DateTimes.of((Date) obj).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + ", " + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == LocalDateTime.class) {
            String dtStr = DateTimes.of((LocalDateTime) obj).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + ", " + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == LocalDate.class) {
            String dtStr = DateTimes.of((LocalDate) obj).toString("yyyy-MM-dd");
            return "to_date(" + quote(dtStr) + ", " + quote("yyyy-mm-dd") + ")";
        }
        if (clazz == LocalTime.class) {
            String dtStr = DateTimes.of((LocalTime) obj).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + ", " + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == byte[].class) {
            return quote("blob:" + FileResource.getSize((byte[]) obj));
        }
        // PostgreSQL array
        if (Object[].class.isAssignableFrom(clazz)) {
            return toPgArrayLiteral((Object[]) obj);
        }
        // I think you wanna save json string
        if (Map.class.isAssignableFrom(clazz) ||
                Collection.class.isAssignableFrom(clazz) ||
                !clazz.getTypeName().startsWith("java.")) {
            String value = Jackson.toJson(obj);
            if (value != null) {
                return safeQuote(value);
            }
            return "null";
        }
        return safeQuote(obj.toString());
    }

    /**
     * Format object value for sql string.
     *
     * @param value object/array value
     * @param quote single quotes or not
     * @return string literal value
     */
    public static String formatObject(Object value, boolean quote) {
        Object[] values = ObjectUtil.toArray(value);
        StringJoiner sb = new StringJoiner(", ");
        if (quote) {
            for (Object v : values) {
                sb.add(quoteFormatValue(v));
            }
        } else {
            for (Object v : values) {
                if (v != null) {
                    sb.add(v.toString());
                }
            }
        }
        return sb.toString();
    }

    /**
     * Parse object value to string literal.
     *
     * @param value object/array value and special value type: {@link Variable}
     * @param quote single quotes or not
     * @return string literal value
     */
    public static String parseValue(Object value, boolean quote) {
        if (value instanceof Variable) {
            return ((Variable) value).stringLiteral();
        }
        return formatObject(value, quote);
    }

    /**
     * Convert array to PostgreSQL array string literal.
     *
     * @param values array
     * @return array string literal
     */
    public static String toPgArrayLiteral(Object[] values) {
        String[] res = new String[values.length];
        for (int i = 0; i < res.length; i++) {
            Object o = values[i];
            if (o != null)
                if (o instanceof String) {
                    res[i] = safeQuote(o.toString());
                } else {
                    res[i] = o.toString();
                }
        }
        return quote("{" + String.join(", ", res) + "}");
    }

    /**
     * Replace sql substring (single quotes) to unique string holder and save the substring map.
     *
     * @param sql sql string
     * @return [sql string with unique string holder, substring map]
     */
    public static Pair<String, Map<String, String>> replaceSqlSubstr(final String sql) {
        //noinspection UnnecessaryUnicodeEscape
        if (!sql.contains("'")) {
            return Pair.of(sql, Collections.emptyMap());
        }
        String noStrSql = sql;
        Map<String, String> mapper = new HashMap<>();
        Matcher m = STR_PATTERN.matcher(sql);
        int i = 0;
        while (m.find()) {
            String str = m.group();
            String holder = SYMBOL + (i++) + SYMBOL;
            noStrSql = noStrSql.replace(str, holder);
            mapper.put(holder, str);
        }
        return Pair.of(noStrSql, mapper);
    }

    /**
     * Trim sql string ends (\t\n\r;).
     *
     * @param sql sql string
     * @return sql string
     */
    public static String trimEnd(String sql) {
        // oracle procedure syntax end with ';' is required.
        if (StringUtil.startsWithIgnoreCase(sql, "begin") && StringUtil.endsWithIgnoreCase(sql, "end")) {
            return sql;
        }
        return sql.replaceAll("([\\s;]*)$", "");
    }

    /**
     * Remove block annotation (/**<span>/</span>).
     *
     * @param sql sql string
     * @return sql without annotation
     */
    public static String removeBlockAnnotation(final String sql) {
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceSqlSubstr(sql);
        String noStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        char[] chars = noStrSql.toCharArray();
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
        String noBSql = sb.toString().replaceAll("\n\\s*\n", "\n");
        for (String key : placeholderMapper.keySet()) {
            noBSql = noBSql.replace(key, placeholderMapper.get(key));
        }
        return noBSql;
    }

    /**
     * Get block annotation (/**<span>/</span>).
     *
     * @param sql sql string
     * @return block annotations
     */
    public static List<String> getBlockAnnotation(final String sql) {
        //noinspection UnnecessaryUnicodeEscape
        String splitter = "\u02ac";
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
                        annotations.append("/").append(splitter);
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
        return Arrays.asList(annotationStr.split(splitter));
    }

    /**
     * Repair sql normal syntax error.<br>
     * e.g.
     * <blockquote>
     * <pre>where and/or/order/limit...</pre>
     * <pre>select ... from ...where</pre>
     * <pre>update ... set  a=b, where</pre>
     * </blockquote>
     *
     * @param sql sql string
     * @return sql string
     */
    public static String repairSyntaxError(final String sql) {
        String result = sql;
        result = SQL_ERR_COMMA_WHERE.matcher(result).replaceAll(" where");
        result = SQL_ERR_WHERE_AND_OR.matcher(result).replaceAll("where ");
        result = SQL_ERR_WHERE_ORDER.matcher(result).replaceAll("$1");
        result = SQL_ERR_WHERE_END.matcher(result).replaceAll("");
        return result;
    }

    /**
     * Highlight sql string with ansi.
     *
     * @param sql sql string
     * @return highlighted sql
     */
    public static String highlightSql(String sql) {
        try {
            Pair<String, Map<String, String>> r = SqlUtil.replaceSqlSubstr(sql);
            String rSql = r.getItem1();
            Pair<List<String>, List<String>> x = StringUtil.regexSplit(rSql, "(?<sp>[\\s,\\[\\]():;])", "sp");
            List<String> maybeKeywords = x.getItem1();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < maybeKeywords.size(); i++) {
                String key = maybeKeywords.get(i);
                if (!key.trim().isEmpty()) {
                    // keywords highlight
                    if (StringUtil.equalsAnyIgnoreCase(key, Keywords.STANDARD)) {
                        maybeKeywords.set(i, Printer.colorful(key, Color.DARK_PURPLE));
                        // functions highlight
                    } else if (rSql.contains(key + "(")) {
                        maybeKeywords.set(i, Printer.colorful(key, Color.BLUE));
                        // number highlight
                    } else if (StringUtil.isNumeric(key)) {
                        maybeKeywords.set(i, Printer.colorful(key, Color.DARK_CYAN));
                        // PostgreSQL function body block highlight
                    } else if (key.equals("$$")) {
                        maybeKeywords.set(i, Printer.colorful(key, Color.DARK_GREEN));
                        // symbol '*' highlight
                    } else if (key.equals("*")) {
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
                    sqlLine[i] = Printer.colorful(line, Color.SILVER);
                } else if (line.contains("--")) {
                    int idx = line.indexOf("--");
                    sqlLine[i] = line.substring(0, idx) + Printer.colorful(line.substring(idx), Color.SILVER);
                }
            }
            colorfulSql = String.join("\n", sqlLine);
            // resolve block annotation
            if (colorfulSql.contains("/*") && colorfulSql.contains("*/")) {
                List<String> annotations = getBlockAnnotation(colorfulSql);
                for (String annotation : annotations) {
                    colorfulSql = colorfulSql.replace(annotation, Printer.colorful(annotation.replaceAll("\033\\[\\d{2}m|\033\\[0m", ""), Color.SILVER));
                }
            }
            return colorfulSql;
        } catch (Exception e) {
            log.error("highlight sql error.", e);
            return sql;
        }
    }

    /**
     * Build highlight sql for console if console is active.
     *
     * @param sql sql string
     * @return normal sql string or highlight sql string
     */
    public static String highlightSqlIfConsole(String sql) {
        if (System.console() != null && System.getenv().get("TERM") != null) {
            return highlightSql(sql);
        }
        return sql;
    }
}
