package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.Baki;

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
    Object handle(Baki baki, String sqlRef, Object args, Method method, Class<?> returnType, Class<?> returnGenericType) throws Throwable;
}
