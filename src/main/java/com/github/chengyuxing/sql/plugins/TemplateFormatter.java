package com.github.chengyuxing.sql.plugins;

@FunctionalInterface
public interface TemplateFormatter {
    /**
     * Non-prepared Sql template ({@code ${[!]key}}) formatter.
     *
     * @param value     value
     * @param isSpecial key start with {@code !} or not
     * @return formatted content
     */
    String format(Object value, boolean isSpecial);
}
