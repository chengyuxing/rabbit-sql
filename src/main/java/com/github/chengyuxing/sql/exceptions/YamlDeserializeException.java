package com.github.chengyuxing.sql.exceptions;

public class YamlDeserializeException extends RuntimeException {
    public YamlDeserializeException(String message) {
        super(message);
    }

    public YamlDeserializeException(String message, Throwable cause) {
        super(message, cause);
    }
}
