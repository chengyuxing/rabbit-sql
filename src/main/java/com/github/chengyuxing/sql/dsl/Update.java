package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.dsl.clause.Where;
import org.jetbrains.annotations.NotNull;

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

    public abstract int saveById(@NotNull T entity);

    public abstract <NEW extends Where<T, NEW>> int save(@NotNull T entity, @NotNull NEW where);
}
