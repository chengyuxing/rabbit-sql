package com.github.chengyuxing.sql.exceptions;

import java.sql.SQLException;

public class UncheckedSqlException extends RuntimeException {
    public UncheckedSqlException(String message, SQLException cause) {
        super(message, cause);
    }

    public UncheckedSqlException(String message) {
        super(message);
    }
}
