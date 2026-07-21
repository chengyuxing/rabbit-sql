package com.github.chengyuxing.sql.types;

import com.github.chengyuxing.sql.annotation.SqlStatementType;

import java.util.HashMap;
import java.util.Map;

/**
 * SQL Execute context.
 */
public class ExecutionContext {
    private final SqlStatementType type;
    private final String sql;
    private final Object args;
    private Object result;
    private final Map<String, Object> status = new HashMap<>();

    public ExecutionContext(SqlStatementType type, String sql, Object args) {
        this.type = type;
        this.sql = sql;
        this.args = args;
    }

    public SqlStatementType getType() {
        return type;
    }

    public String getSql() {
        return sql;
    }

    public Object getArgs() {
        return args;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public void setState(String key, Object value) {
        status.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T getState(String key) {
        return (T) status.get(key);
    }
}
