package com.github.chengyuxing.sql.exceptions;

/**
 * sql运行时异常
 */
public class SqlRuntimeException extends RuntimeException {
    public SqlRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlRuntimeException(String message) {
        super(message);
    }
}
