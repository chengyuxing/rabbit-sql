package com.github.chengyuxing.sql.exceptions;

/**
 * 动态sql脚本错误异常
 */
public class DynamicSQLException extends RuntimeException {
    public DynamicSQLException(String message) {
        super(message);
    }

    public DynamicSQLException(String message, Throwable cause) {
        super(message, cause);
    }
}
