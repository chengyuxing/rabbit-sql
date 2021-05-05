package com.github.chengyuxing.sql.transaction;

import java.sql.Connection;

/**
 * 事务级别
 */
public enum Level {
    NONE(Connection.TRANSACTION_NONE, "none"),
    READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED, "read_unCommit"),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED, "read_commit"),
    REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ, "repeatable_read"),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE, "serializable");

    private final int value;
    private final String name;

    Level(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public String getName() {
        return name;
    }
}
