package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface TemplateFormatter {
    /**
     * Non-prepared Sql template ({@code ${[!]key}}) formatter.
     *
     * @param value     value
     * @param isSpecial key start with {@code !} or not
     * @return formatted content
     */
    @NotNull String format(Object value, boolean isSpecial);
}
