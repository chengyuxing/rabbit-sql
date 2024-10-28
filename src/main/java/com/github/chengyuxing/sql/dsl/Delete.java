package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.dsl.clause.Where;
import org.jetbrains.annotations.NotNull;

public abstract class Delete<T> {
    public abstract int saveById(@NotNull T entity);

    public abstract <NEW extends Where<T, NEW>> int save(@NotNull T entity, @NotNull NEW where);
}
