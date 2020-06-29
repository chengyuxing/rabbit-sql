package rabbit.sql.types;

import rabbit.sql.support.IOutParam;

import java.sql.Types;

/**
 * 出参枚举类型
 */
public enum OUTParamType implements IOutParam {
    REF_CURSOR(Types.REF_CURSOR, "cursor"),
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

    OUTParamType(int typeNumber, String name) {
        this.typeNumber = typeNumber;
        this.name = name;
    }

    @Override
    public int getTypeNumber() {
        return typeNumber;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "OUTParamType{" +
                "name='" + name + '\'' +
                '}';
    }
}
