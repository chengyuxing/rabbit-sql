package com.github.chengyuxing.sql.exceptions;

import java.sql.SQLException;
import java.util.Objects;

public class UncheckedSqlException extends RuntimeException {
    public UncheckedSqlException(String message, SQLException cause) {
        super(message, Objects.requireNonNull(cause));
    }

    public UncheckedSqlException(String message) {
        super(message);
    }
}
