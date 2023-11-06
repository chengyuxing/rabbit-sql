package com.github.chengyuxing.sql.types;

import com.github.chengyuxing.sql.utils.SqlUtil;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Variable interface for format object to custom string literal.
 *
 * @see ExamplePgArray
 */
@FunctionalInterface
public interface Variable {
    /**
     * Convert object value to string literal.
     *
     * @return string literal
     */
    String stringLiteral();

    /**
     * Example handle postgresql array string literal.<br>
     * e.g. Object[]{"a", "b", "c"} {@code ->} {'a', 'b', 'c'}
     */
    class ExamplePgArray implements Variable {
        private final Object[] array;

        public ExamplePgArray(Object[] array) {
            this.array = array;
        }

        @Override
        public String stringLiteral() {
            return Stream.of(array).map(item -> {
                if (item instanceof String) {
                    return SqlUtil.safeQuote((String) item);
                }
                return item.toString();
            }).collect(Collectors.joining(", ", "{", "}"));
        }
    }
}
