package com.github.chengyuxing.sql.exceptions;

public class IllegalXQLSegmentNameException extends RuntimeException {
    public IllegalXQLSegmentNameException(String message) {
        super(message);
    }

    public IllegalXQLSegmentNameException(String message, Throwable cause) {
        super(message, cause);
    }
}
