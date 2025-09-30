package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.StringFormatter;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.plugins.TemplateFormatter;
import com.github.chengyuxing.sql.types.Variable;

import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL util.
 */
public class SqlUtil {
    //language=RegExp
    public static final Pattern STR_PATTERN = Pattern.compile("'(''|[^'])*'", Pattern.MULTILINE);
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
     * String value with single safe quotes.
     *
     * @param value string value
     * @return string value with single safe quotes
     */
    public static String safeQuote(String value) {
        if (Objects.isNull(value)) {
            return "null";
        }
        if (value.contains("'")) {
            value = value.replace("'", "''");
        }
        return "'" + value + "'";
    }

    /**
     * Object value safe format to string literal value.
     *
     * @param obj object value
     * @return formatted string literal value
     */
    public static String safeQuote(Object obj) {
        if (Objects.isNull(obj)) {
            return "null";
        }
        if (obj instanceof String || obj instanceof Character) {
            return safeQuote(obj.toString());
        }
        if (ReflectUtil.isBasicType(obj)) {
            return obj.toString();
        }
        return safeQuote(obj.toString());
    }

    /**
     * Parse object value to string literal.
     *
     * @param value object/array value and special value type: {@link Variable}
     * @param quote single quotes or not
     * @return string literal value
     */
    public static String parseValue(Object value, boolean quote) {
        if (Objects.isNull(value)) {
            return "null";
        }
        if (value instanceof Variable) {
            return ((Variable) value).stringLiteral();
        }
        Object[] values = ObjectUtil.toArray(value);
        StringJoiner sb = new StringJoiner(", ");
        if (quote) {
            for (Object v : values) {
                sb.add(safeQuote(v));
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
     * Trim sql string ends ({@code \t\n\r;}).
     *
     * @param sql sql string
     * @return sql string
     */
    public static String trimEnd(String sql) {
        // oracle pl/sql syntax end with ';' is required.
        if (StringUtil.startsWithsIgnoreCase(sql, "begin", "declare")) {
            return sql;
        }
        return sql.replaceAll("([\\s;]*)$", "");
    }
}
