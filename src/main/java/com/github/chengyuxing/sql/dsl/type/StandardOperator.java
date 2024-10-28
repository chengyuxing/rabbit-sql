package com.github.chengyuxing.sql.dsl.type;

public enum StandardOperator implements Operator {
    EQ(" = "),
    NEQ(" <> "),
    GT(" > "),
    GTE(" >= "),
    LT(" < "),
    LTE(" <= "),
    LIKE(" like "),
    NOT_LIKE(" not like "),
    IN(" in "),
    NOT_IN(" not in "),
    BETWEEN(" between "),
    NOT_BETWEEN(" not between "),
    IS(" is "),
    IS_NOT(" is not "),
    LOGIC_AND(" and "),
    LOGIC_OR(" or "),
    ;
    private final String value;

    StandardOperator(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }
}
