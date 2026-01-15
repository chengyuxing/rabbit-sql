package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;

import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.FMT;

/**
 * SQL util.
 */
public class SqlUtil {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*)*$");
    private static BiFunction<Object, Boolean, String> formatter = SqlUtil::toSqlLiteral;

    /**
     * Set and replace the global formatter
     *
     * @param formatter new formatter
     * @see #toSqlLiteral(Object, boolean)
     */
    public static void setFormatter(BiFunction<Object, Boolean, String> formatter) {
        SqlUtil.formatter = formatter;
    }

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
     *  idArr: ["a", "b", "c"]
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
     * @param template sql string with template variable
     * @param data     data
     * @return formatted sql string
     * @see #toSqlLiteral(Object, boolean)
     */
    public static String formatSql(final String template, final Map<String, ?> data) {
        return FMT.format(template, data, formatter);
    }

    /**
     * Parse object value to sql literal.
     *
     * @param value       object/array value
     * @param isSafeQuote single quotes or not
     * @return string literal value
     */
    public static String toSqlLiteral(Object value, boolean isSafeQuote) {
        if (value == null) {
            return "null";
        }
        Object[] values = ObjectUtil.toArray(value);
        StringJoiner sb = new StringJoiner(", ");
        if (isSafeQuote) {
            for (Object v : values) {
                if (v == null) {
                    sb.add("null");
                    continue;
                }
                if (ReflectUtil.isBasicType(v) && !(v instanceof String) && !(v instanceof Character)) {
                    sb.add(v.toString());
                    continue;
                }
                sb.add("'" + v.toString().replace("'", "''") + "'");
            }
        } else {
            for (Object v : values) {
                if (v == null) {
                    sb.add("null");
                    continue;
                }
                sb.add(v.toString());
            }
        }
        return sb.toString();
    }

    public static boolean isIdentifier(String s) {
        return s != null && IDENTIFIER_PATTERN.matcher(s).matches();
    }

    public static void assertInvalidIdentifier(String s) {
        if (!isIdentifier(s)) {
            throw new IllegalArgumentException("Illegal SQL identifier: " + s);
        }
    }
}
