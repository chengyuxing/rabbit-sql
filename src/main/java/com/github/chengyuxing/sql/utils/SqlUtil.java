package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.MostDateTime;
import com.github.chengyuxing.common.StringFormatter;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.support.TemplateFormatter;
import com.github.chengyuxing.sql.types.Variable;

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
    //language=RegExp
    public static final Pattern STR_PATTERN = Pattern.compile("'(''|[^'])*'", Pattern.MULTILINE);
    //language=RegExp
    public static final Pattern SQL_ERR_COMMA_WHERE = Pattern.compile(",\\s*(where\\W+)", Pattern.CASE_INSENSITIVE);
    //language=RegExp
    public static final Pattern SQL_ERR_WHERE_AND_OR = Pattern.compile("(\\s+where\\s+)(and|or)(\\W+)", Pattern.CASE_INSENSITIVE);
    //language=RegExp
    public static final Pattern SQL_ERR_WHERE_ORDER = Pattern.compile("\\s+where(\\s+order|\\s+limit|\\s+group|\\s+union|\\s*\\))\\s+", Pattern.CASE_INSENSITIVE);
    //language=RegExp
    public static final Pattern SQL_ERR_WHERE_END = Pattern.compile("\\s+where\\s*$", Pattern.CASE_INSENSITIVE);

    public static final StringFormatter FMT = new StringFormatter();

    /**
     * Format sql string, e.g.
     * <p>sql statement:</p>
     * <blockquote>
     * <pre>select ${ fields } from test.user
     * where ${  cnd}
     * and id in (${!idArr})
     * or id = ${!idArr.1}</pre>
     * </blockquote>
     * <p>args:</p>
     * <blockquote>
     * <pre>
     * {
     *  fields: "id, name",
     *  cnd: "name = 'cyx'",
     *  idArr: ["a", "b", "c"],
     * }</pre>
     * </blockquote>
     * <p>result:</p>
     * <blockquote>
     * <pre>select id, name from test.user
     * where name = 'cyx'
     * and id in ('a', 'b', 'c')
     * or id = 'b'</pre>
     * </blockquote>
     *
     * @param template       sql string with template variable
     * @param data           data
     * @param valueFormatter function for value format to string literal value
     * @return formatted sql string
     * @see Variable
     */
    public static String formatSql(final String template, final Map<String, ?> data, TemplateFormatter valueFormatter) {
        return FMT.format(template, data, valueFormatter::format);
    }

    /**
     * Format sql string, e.g.
     * <p>sql statement:</p>
     * <blockquote>
     * <pre>select ${ fields } from test.user
     * where ${  cnd}
     * and id in (${!idArr})
     * or id = ${!idArr.1}
     * and dt &lt;= ${!now}</pre>
     * </blockquote>
     * <p>args:</p>
     * <blockquote>
     * <pre>
     * {
     *  fields: "id, name",
     *  cnd: "name = 'cyx'",
     *  idArr: ["a", "b", "c"],
     *  now: {@link LocalDateTime#now() LocalDateTime.now()}
     * }</pre>
     * </blockquote>
     * <p>result:</p>
     * <blockquote>
     * <pre>select id, name from test.user
     * where name = 'cyx'
     * and id in ('a', 'b', 'c')
     * or id = 'b'
     * and dt &lt;= to_timestamp('2023-11-14 19:14:34', 'yyyy-mm-dd hh24:mi:ss')</pre>
     * </blockquote>
     * Notice: If {@link Variable} type detected, {@link Variable#stringLiteral() stringLiteral()} method will be invoked.
     *
     * @param template sql string with template variable
     * @param data     data
     * @return formatted sql string
     * @see Variable
     */
    public static String formatSql(final String template, final Map<String, ?> data) {
        return formatSql(template, data, SqlUtil::parseValue);
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
            String dtStr = MostDateTime.of((Date) obj).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + ", " + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == LocalDateTime.class) {
            String dtStr = MostDateTime.of((LocalDateTime) obj).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + ", " + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == LocalDate.class) {
            String dtStr = MostDateTime.of((LocalDate) obj).toString("yyyy-MM-dd");
            return "to_date(" + quote(dtStr) + ", " + quote("yyyy-mm-dd") + ")";
        }
        if (clazz == LocalTime.class) {
            String dtStr = MostDateTime.of((LocalTime) obj).toString("yyyy-MM-dd HH:mm:ss");
            return "to_timestamp(" + quote(dtStr) + ", " + quote("yyyy-mm-dd hh24:mi:ss") + ")";
        }
        if (clazz == byte[].class) {
            return quote("blob:" + FileResource.formatFileSize((byte[]) obj));
        }
        // PostgreSQL array
        if (Object[].class.isAssignableFrom(clazz)) {
            return toPgArrayLiteral((Object[]) obj);
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
        if (Objects.isNull(value)) {
            return "null";
        }
        Object[] values = ObjectUtil.toArray(value);
        StringJoiner sb = new StringJoiner(", ");
        if (quote) {
            for (Object v : values) {
                sb.add(quoteFormatValue(v));
            }
        } else {
            for (Object v : values) {
                if (Objects.isNull(v)) {
                    sb.add("null");
                    continue;
                }
                sb.add(v.toString());
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
     * Escape sql substring (single quotes) to unique string holder and save the substring map.
     *
     * @param sql sql string
     * @return [sql string with unique string holder, substring map]
     */
    public static Pair<String, Map<String, String>> escapeSubstring(final String sql) {
        //noinspection UnnecessaryUnicodeEscape
        if (!sql.contains("'")) {
            return Pair.of(sql, Collections.emptyMap());
        }
        Matcher m = STR_PATTERN.matcher(sql);
        Map<String, String> map = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        int pos = 0;
        while (m.find()) {
            int start = m.start();
            int end = m.end();
            String str = m.group();
            String holder = UUID.randomUUID().toString();
            map.put(holder, str);
            sb.append(sql, pos, start).append(holder);
            pos = end;
        }
        sb.append(sql, pos, sql.length());
        return Pair.of(sb.toString(), map);
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
     * Repair sql normal syntax error.<br>
     * e.g.
     * <blockquote>
     * <pre>
     * where and/or/order/limit...;
     * select ... from ...where;
     * update ... set  a=b, where;</pre>
     * </blockquote>
     *
     * @param sql sql string
     * @return sql string
     */
    public static String repairSyntaxError(final String sql) {
        String result = sql;
        result = SQL_ERR_COMMA_WHERE.matcher(result).replaceAll(" $1");
        result = SQL_ERR_WHERE_AND_OR.matcher(result).replaceAll("$1$3");
        result = SQL_ERR_WHERE_ORDER.matcher(result).replaceAll("$1 ");
        result = SQL_ERR_WHERE_END.matcher(result).replaceAll("");
        return result;
    }
}
