package com.github.chengyuxing.sql.types;

/**
 * Variable interface for format object to custom string literal.
 */
@FunctionalInterface
public interface Variable {
    /**
     * Convert object value to string literal.
     *
     * @return string literal
     */
    String stringLiteral();
}
