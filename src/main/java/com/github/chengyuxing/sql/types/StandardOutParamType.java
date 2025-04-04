package com.github.chengyuxing.sql.types;

import java.sql.Types;

/**
 * Store procedure/function OUT parameter type.
 */
public enum StandardOutParamType implements OutParamType {
    REF_CURSOR(Types.REF_CURSOR, "ref_cursor"),
    ORACLE_CURSOR(-10, "oracle_cursor"),
    VARCHAR(Types.VARCHAR, "varchar"),
    NVARCHAR(Types.NVARCHAR, "nvarchar"),
    INTEGER(Types.INTEGER, "integer"),
    ARRAY(Types.ARRAY, "array"),
    BLOB(Types.BLOB, "blob"),
    BOOLEAN(Types.BOOLEAN, "boolean"),
    TIMESTAMP(Types.TIMESTAMP, "timestamp"),
    OTHER(Types.OTHER, "other");

    private final int typeNumber;
    private final String name;

    StandardOutParamType(int typeNumber, String name) {
        this.typeNumber = typeNumber;
        this.name = name;
    }

    @Override
    public int typeNumber() {
        return typeNumber;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
