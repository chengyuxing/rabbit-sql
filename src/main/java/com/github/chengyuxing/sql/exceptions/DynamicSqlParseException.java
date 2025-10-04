package com.github.chengyuxing.sql.exceptions;

public class DynamicSqlParseException extends RuntimeException {
    public DynamicSqlParseException(String message) {
        super(message);
    }

    public DynamicSqlParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
