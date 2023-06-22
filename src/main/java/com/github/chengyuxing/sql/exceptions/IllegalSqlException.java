package com.github.chengyuxing.sql.exceptions;

/**
 * 非法sql异常
 */
public class IllegalSqlException extends RuntimeException {
    public IllegalSqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalSqlException(String message) {
        super(message);
    }
}
