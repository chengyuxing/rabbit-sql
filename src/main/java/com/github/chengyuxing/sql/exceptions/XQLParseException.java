package com.github.chengyuxing.sql.exceptions;

public class XQLParseException extends RuntimeException {
    public XQLParseException(String message) {
        super(message);
    }

    public XQLParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
