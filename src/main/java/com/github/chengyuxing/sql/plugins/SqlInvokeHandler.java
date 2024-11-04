package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.Baki;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;

@FunctionalInterface
public interface SqlInvokeHandler {
    /**
     * Handle sql execution result
     *
     * @param baki              baki
     * @param sqlRef            sql reference (&amp;alias.sqlName)
     * @param args              args
     * @param method            invoked method
     * @param returnType        method return type
     * @param returnGenericType method return generic type
     * @return execute result
     * @throws Throwable throwable
     */
    Object handle(@NotNull Baki baki,
                  @NotNull String sqlRef,
                  @NotNull Object args,
                  @NotNull Method method,
                  @NotNull Class<?> returnType,
                  @NotNull Class<?> returnGenericType) throws Throwable;
}
