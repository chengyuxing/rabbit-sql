package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.common.TiFunction;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.annotation.SqlStatementType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Method;

@FunctionalInterface
public interface SqlInvokeHandler {
    /**
     * Handle sql execution result.
     *
     * @param type SQL type
     * @return SQL invoke handler function object
     */
    @Nullable TiFunction<@NotNull Baki, @NotNull Method, @NotNull Object[], Object> invoke(SqlStatementType type);
}
