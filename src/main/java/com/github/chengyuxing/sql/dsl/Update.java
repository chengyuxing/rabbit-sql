package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.dsl.clause.Where;

import java.util.function.Function;

public abstract class Update<T> {
    protected boolean ignoreNull = false;

    public Update<T> ignoreNull(boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
        return this;
    }

    public Update<T> ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    public abstract int byId();

    public abstract int by(Function<Where<T>, Where<T>> where);
}
