package com.github.chengyuxing.sql.util;

import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.common.util.ReflectUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.util.StringUtils.FMT;

/**
 * SQL util.
 */
public class SqlUtils {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z_][\\w$]*(\\.[A-Za-z_][\\w$]*)*$");
    private static BiFunction<Object, Boolean, String> formatter = SqlUtils::toSqlLiteral;

    /**
     * Set and replace the global formatter
     *
     * @param formatter new formatter
     * @see #toSqlLiteral(Object, boolean)
     */
    public static void setFormatter(BiFunction<Object, Boolean, String> formatter) {
        SqlUtils.formatter = formatter;
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
        Iterable<?> values = ValueUtils.asIterable(value);
        StringJoiner sb = new StringJoiner(", ");
        if (isSafeQuote) {
            for (Object v : values) {
                if (v == null) {
                    sb.add("null");
                    continue;
                }
                if (ReflectUtils.isBasicType(v) && !(v instanceof String) && !(v instanceof Character)) {
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

    /**
     * Checks if the provided string is a valid SQL identifier.
     *
     * @param s the string to check
     * @return true if the string is a valid SQL identifier, false otherwise
     */
    public static boolean isIdentifier(String s) {
        return s != null && IDENTIFIER_PATTERN.matcher(s).matches();
    }

    /**
     * Throws an IllegalArgumentException if the provided string is not a valid SQL identifier.
     *
     * @param s the string to be checked as a valid SQL identifier
     * @throws IllegalArgumentException if the provided string is not a valid SQL identifier
     */
    public static void assertInvalidIdentifier(String s) {
        if (!isIdentifier(s)) {
            throw new IllegalArgumentException("Illegal SQL identifier: " + s);
        }
    }

    /**
     * Searches for the start index of a whole line comment in the provided string.
     * A whole line comment is defined as starting with '--' and not preceded by any non-whitespace characters.
     *
     * @param line the string to search within
     * @return the index of the first character of the '--' if a whole line comment is found, otherwise -1
     */
    public static int indexOfWholeLineComment(@NotNull String line) {
        int len = line.length();
        for (int i = 0; i < len - 1; i++) {
            char c = line.charAt(i);
            if (c == ' ' || c == '\t' || Character.isWhitespace(c)) {
                continue;
            }
            if (c == '-' && line.charAt(i + 1) == '-') {
                return i;
            }
            return -1;
        }
        return -1;
    }
}
