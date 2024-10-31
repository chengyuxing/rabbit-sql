package com.github.chengyuxing.sql.dsl;

import com.github.chengyuxing.sql.dsl.clause.Where;

import java.util.function.Function;

public interface Delete<T> {
    int byId();

    int by(Function<Where<T>, Where<T>> where);
}
