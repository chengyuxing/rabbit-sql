package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.sql.XQLInvocationHandler;
import com.github.chengyuxing.sql.annotation.XQLMapper;

import java.lang.reflect.Proxy;

public class XQLMapperUtil {
    @SuppressWarnings("unchecked")
    public static <T> T getProxyInstance(Class<T> clazz, XQLInvocationHandler xqlInvocationHandler) throws IllegalAccessException {
        if (!clazz.isInterface()) {
            throw new IllegalAccessException("Not interface: " + clazz.getName());
        }
        if (!clazz.isAnnotationPresent(XQLMapper.class)) {
            throw new IllegalAccessException(clazz + " should be annotated with @" + XQLMapper.class.getSimpleName());
        }
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, xqlInvocationHandler);
    }
}
