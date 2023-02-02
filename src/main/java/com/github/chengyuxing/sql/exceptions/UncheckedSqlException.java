package com.github.chengyuxing.sql.exceptions;

import java.sql.SQLException;
import java.util.Objects;

/**
 * sql运行时异常
 */
public class UncheckedSqlException extends RuntimeException {
    public UncheckedSqlException(String message, SQLException cause) {
        super(message, Objects.requireNonNull(cause));
    }

    public UncheckedSqlException(String message) {
        super(message);
    }
}
