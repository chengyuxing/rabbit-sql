package com.github.chengyuxing.sql.types;

import com.github.chengyuxing.sql.utils.SqlUtil;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Variable interface for format object to custom string literal.
 *
 * @see ExampleDBArray
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
    class ExampleDBArray implements Variable {
        private final Object[] array;
        private final DatabaseMetaData metaData;

        public ExampleDBArray(Object[] array, DatabaseMetaData metaData) {
            this.array = array;
            this.metaData = metaData;
        }

        @Override
        public String stringLiteral() {
            try {
                if (metaData.getDatabaseProductName().equalsIgnoreCase("postgresql")) {
                    return Stream.of(array).map(item -> {
                        if (item instanceof String) {
                            return SqlUtil.safeQuote((String) item);
                        }
                        return item.toString();
                    }).collect(Collectors.joining(", ", "{", "}"));
                }
                return "other database implementation...";
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
