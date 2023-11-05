package com.github.chengyuxing.sql.exceptions;

public class IllegalSqlException extends RuntimeException {
    public IllegalSqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalSqlException(String message) {
        super(message);
    }
}
