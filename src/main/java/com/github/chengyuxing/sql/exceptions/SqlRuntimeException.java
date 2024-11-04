package com.github.chengyuxing.sql.exceptions;

public class SqlRuntimeException extends RuntimeException {
    public SqlRuntimeException(String message) {
        super(message);
    }

    public SqlRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
