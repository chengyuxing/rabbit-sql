package com.github.chengyuxing.sql.util;

import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.common.util.ReflectUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.regex.Pattern;

/**
 * SQL util.
 */
public class SqlUtils {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z_][\\w$]*(\\.[A-Za-z_][\\w$]*)*$");

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
