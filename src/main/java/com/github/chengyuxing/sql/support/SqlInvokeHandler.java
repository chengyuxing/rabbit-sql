package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.sql.BakiDao;

import java.lang.reflect.Method;

@FunctionalInterface
public interface SqlInvokeHandler {
    /**
     * Handle sql execution result
     *
     * @param bakiDao           BakiDao
     * @param sqlRef            sql reference (&alias.sqlName)
     * @param args              args
     * @param method            invoked method
     * @param returnType        method return type
     * @param returnGenericType method return generic type
     * @return execute result
     * @throws Throwable throwable
     */
    Object handle(BakiDao bakiDao, String sqlRef, Object args, Method method, Class<?> returnType, Class<?> returnGenericType) throws Throwable;
}
