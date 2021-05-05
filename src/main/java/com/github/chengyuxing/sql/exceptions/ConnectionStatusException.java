package com.github.chengyuxing.sql.exceptions;

public class ConnectionStatusException extends RuntimeException{
    public ConnectionStatusException(String message) {
        super(message);
    }

    public ConnectionStatusException(String message, Throwable cause) {
        super(message, cause);
    }
}
