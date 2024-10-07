package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.sql.annotation.*;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.support.SqlInvokeHandler;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.SaveExecutor;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.annotation.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("SqlSourceToSinkFlow")
public abstract class XQLInvocationHandler implements InvocationHandler {
    // language=Regexp
    public static final String QUERY_PATTERN = "^(?:select|query|find|get|fetch|search)[^a-z]\\w*";
    // language=Regexp
    public static final String INSERT_PATTERN = "^(?:insert|save|add|append)[^a-z]\\w*";
    // language=Regexp
    public static final String UPDATE_PATTERN = "^(?:update|modify|change)[^a-z]\\w*";
    // language=Regexp
    public static final String DELETE_PATTERN = "^(?:delete|remove)[^a-z]\\w*";
    // language=Regexp
    public static final String CALL_PATTERN = "^(?:call|proc|func)[^a-z]\\w*";

    private final ClassLoader classLoader = this.getClass().getClassLoader();

    protected abstract BakiDao baki();

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?> clazz = method.getDeclaringClass();
        Class<?> returnType = method.getReturnType();
        Class<?> returnGenericType = getReturnGenericType(method);

        Object myArgs = resolveArgs(method, args);

        if (method.isAnnotationPresent(Insert.class) ||
                method.isAnnotationPresent(Update.class) ||
                method.isAnnotationPresent(Delete.class)) {
            return handleNotXqlMappingMethod(myArgs, method, returnType);
        }

        if (method.isAnnotationPresent(Procedure.class)) {
            Procedure procedure = method.getDeclaredAnnotation(Procedure.class);
            return handleProcedure(procedure.value(), myArgs, method, returnType);
        }

        if (method.isAnnotationPresent(com.github.chengyuxing.sql.annotation.Function.class)) {
            com.github.chengyuxing.sql.annotation.Function function = method.getDeclaredAnnotation(com.github.chengyuxing.sql.annotation.Function.class);
            return handleProcedure(function.value(), myArgs, method, returnType);
        }

        String alias = clazz.getDeclaredAnnotation(XQLMapper.class).value();
        String sqlName = method.getName();

        Type sqlType = detectSQLTypeByMethodPrefix(sqlName);

        if (method.isAnnotationPresent(XQL.class)) {
            XQL xql = method.getDeclaredAnnotation(XQL.class);
            if (!xql.value().trim().isEmpty()) {
                sqlName = xql.value();
            }
            sqlType = xql.type();
        }

        XQLFileManager.Resource xqlResource = baki().getXqlFileManager().getResource(alias);
        if (xqlResource == null) {
            throw new IllegalAccessException("XQL file alias '" + alias + "' not found at: " + clazz.getName());
        }
        if (!xqlResource.getEntry().containsKey(sqlName)) {
            throw new IllegalAccessException("SQL name [" + sqlName + "] not found at: " + clazz.getName() + "#" + method.getName());
        }

        String sqlRef = "&" + alias + "." + sqlName;

        if (handlers().containsKey(sqlType)) {
            SqlInvokeHandler handler = handlers().get(sqlType);
            return handler.handle(baki(), sqlRef, myArgs, method, returnType, returnGenericType);
        }

        switch (sqlType) {
            case query:
                return handleQuery(alias, sqlName, myArgs, method, returnType, returnGenericType);
            case insert:
            case update:
            case delete:
                return handleModify(sqlRef, myArgs, method, returnType);
            case procedure:
            case function:
                return handleProcedure(sqlRef, myArgs, method, returnType);
            case ddl:
            case plsql:
            case unset:
                return handleNormal(sqlRef, myArgs, method, returnType);
            default:
                throw new IllegalAccessException("SQL type [" + sqlType + "] not supported");
        }
    }

    protected Map<Type, SqlInvokeHandler> handlers() {
        return baki().getXqlMappingHandlers();
    }

    protected Type detectSQLTypeByMethodPrefix(String method) {
        if (method.matches(QUERY_PATTERN)) {
            return Type.query;
        }
        if (method.matches(INSERT_PATTERN)) {
            return Type.insert;
        }
        if (method.matches(UPDATE_PATTERN)) {
            return Type.update;
        }
        if (method.matches(DELETE_PATTERN)) {
            return Type.delete;
        }
        if (method.matches(CALL_PATTERN)) {
            return Type.procedure;
        }
        return Type.unset;
    }

    @SuppressWarnings("unchecked")
    protected Object handleNotXqlMappingMethod(Object myArgs, Method method, Class<?> returnType) {
        if (returnType != Integer.class && returnType != int.class) {
            throw new IllegalStateException(method.getName() + " return type must be Integer or int");
        }
        if (method.isAnnotationPresent(Insert.class)) {
            Insert insert = method.getDeclaredAnnotation(Insert.class);
            SaveExecutor<Object> e = baki().insert(insert.value())
                    .ignoreNull(insert.ignoreNull())
                    .safe(insert.safe());
            if (myArgs instanceof Collection) {
                return e.save((Collection<? extends Map<String, ?>>) myArgs);
            }
            return e.save((Map<String, ?>) myArgs);
        }
        if (method.isAnnotationPresent(Update.class)) {
            Update update = method.getDeclaredAnnotation(Update.class);
            SaveExecutor<Object> e = baki().update(update.value(), update.where())
                    .ignoreNull(update.ignoreNull())
                    .safe(update.safe());
            if (myArgs instanceof Collection) {
                return e.save((Collection<? extends Map<String, ?>>) myArgs);
            }
            return e.save((Map<String, ?>) myArgs);
        }
        if (method.isAnnotationPresent(Delete.class)) {
            Delete delete = method.getDeclaredAnnotation(Delete.class);
            SaveExecutor<Object> e = baki().delete(delete.value(), delete.where());
            if (myArgs instanceof Collection) {
                return e.save((Collection<? extends Map<String, ?>>) myArgs);
            }
            return e.save((Map<String, ?>) myArgs);
        }
        return null;
    }

    protected DataRow handleNormal(String sqlRef, Object args, Method method, Class<?> returnType) {
        if (!Map.class.isAssignableFrom(returnType)) {
            throw new IllegalStateException(method.getName() + " return type must be Map");
        }
        //noinspection unchecked
        return baki().of(sqlRef).execute((Map<String, Object>) args);
    }

    protected int handleModify(String sqlRef, Object args, Method method, Class<?> returnType) {
        if (returnType != Integer.class && returnType != int.class) {
            throw new IllegalStateException(method.getName() + " return type must be Integer or int");
        }
        if (args instanceof Map) {
            //noinspection unchecked
            return baki().of(sqlRef).execute((Map<String, Object>) args).getFirstAs();
        }
        //noinspection unchecked
        return baki().of(sqlRef).executeBatch((Collection<? extends Map<String, ?>>) args);
    }

    protected DataRow handleProcedure(String sqlRef, Object args, Method method, Class<?> returnType) {
        if (!Map.class.isAssignableFrom(returnType)) {
            throw new IllegalStateException(method.getName() + " return type must be map or DataRow");
        }
        if (args instanceof Collection) {
            throw new IllegalArgumentException(method.getName() + " args must not be Collection");
        }
        Map<String, Param> myPaArgs = new HashMap<>();
        //noinspection unchecked
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) args).entrySet()) {
            myPaArgs.put(entry.getKey(), (Param) entry.getValue());
        }
        return baki().of(sqlRef).call(myPaArgs);
    }

    protected Object handleQuery(String alias, String sqlName, Object args, Method method, Class<?> returnType, Class<?> genericType) {
        if (args instanceof Collection) {
            throw new IllegalArgumentException(method.getName() + " args must not be Collection");
        }
        @SuppressWarnings("unchecked") QueryExecutor qe = baki().query("&" + alias + "." + sqlName).args((Map<String, Object>) args);
        if (returnType == Stream.class) {
            return qe.stream().map(dataRowMapping(genericType));
        }
        if (returnType == List.class) {
            try (Stream<DataRow> s = qe.stream()) {
                return s.map(dataRowMapping(genericType)).collect(Collectors.toList());
            }
        }
        if (returnType == Set.class) {
            try (Stream<DataRow> s = qe.stream()) {
                return s.map(dataRowMapping(genericType)).collect(Collectors.toSet());
            }
        }
        if (returnType == int.class || returnType == Integer.class) {
            return qe.findFirstRow().getInt(0);
        }
        if (returnType == long.class || returnType == Long.class) {
            return qe.findFirstRow().getLong(0);
        }
        if (returnType == double.class || returnType == Double.class) {
            return qe.findFirstRow().getDouble(0);
        }
        if (Map.class.isAssignableFrom(returnType)) {
            return qe.findFirstRow();
        }
        if (returnType == Optional.class) {
            return qe.findFirst().map(dataRowMapping(genericType));
        }
        if (returnType == IPageable.class) {
            if (method.isAnnotationPresent(CountQuery.class)) {
                CountQuery countQuery = method.getDeclaredAnnotation(CountQuery.class);
                return qe.pageable().count("&" + alias + "." + countQuery.value());
            }
            return qe.pageable();
        }
        if (returnType == PagedResource.class) {
            if (method.isAnnotationPresent(CountQuery.class)) {
                CountQuery countQuery = method.getDeclaredAnnotation(CountQuery.class);
                return qe.pageable().count("&" + alias + "." + countQuery.value())
                        .collect(dataRowMapping(genericType));
            }
            return qe.pageable().collect(dataRowMapping(genericType));
        }
        if (!returnType.getName().startsWith("java.")) {
            return qe.findFirstEntity(returnType);
        }
        return null;
    }

    /**
     * Get method first return generic type
     *
     * @param method method
     * @return generic type
     * @throws ClassNotFoundException java bean entity class not found
     */
    protected Class<?> getReturnGenericType(Method method) throws ClassNotFoundException {
        Class<?> genericType = null;
        java.lang.reflect.Type genericReturnType = method.getGenericReturnType();
        if (genericReturnType instanceof ParameterizedType) {
            java.lang.reflect.Type[] actualTypeArguments = ((ParameterizedType) genericReturnType).getActualTypeArguments();
            if (actualTypeArguments.length == 1) {
                java.lang.reflect.Type actualTypeArgument = actualTypeArguments[0];
                if (actualTypeArgument instanceof ParameterizedType) {
                    genericType = (Class<?>) ((ParameterizedType) actualTypeArgument).getRawType();
                } else {
                    genericType = classLoader.loadClass(actualTypeArgument.getTypeName());
                }
            }
        }
        return genericType;
    }

    /**
     * DataRow mapping function.
     *
     * @param genericType method return generic type
     * @return function
     */
    protected Function<DataRow, Object> dataRowMapping(Class<?> genericType) {
        return d -> {
            if (genericType.isAssignableFrom(d.getClass())) {
                return d;
            }
            return ObjectUtil.map2entity(d, genericType);
        };
    }

    /**
     * Resolve args to Map or Collection.
     *
     * @param method method
     * @param args   args
     * @return Map or Collection
     */
    protected Object resolveArgs(Method method, Object[] args) {
        Parameter[] parameters = method.getParameters();
        if (parameters.length == 0) {
            return Collections.emptyMap();
        }
        if (parameters.length == 1) {
            Annotation[] annotations = parameters[0].getAnnotations();
            if (annotations.length == 0) {
                Object arg = args[0];
                if (arg instanceof Collection) {
                    List<Object> myArgs = new ArrayList<>(((Collection<?>) arg).size());
                    for (Object o : (Collection<?>) arg) {
                        if (o instanceof Map) {
                            myArgs.add(o);
                            continue;
                        }
                        if (!arg.getClass().getName().startsWith("java.")) {
                            myArgs.add(ObjectUtil.entity2map(arg, HashMap::new));
                            continue;
                        }
                        throw new IllegalArgumentException(method.getName() + " unsupported arg type: " + arg.getClass().getName());
                    }
                    return myArgs;
                }
                if (arg instanceof Map) {
                    return arg;
                }
                if (!ReflectUtil.isBasicType(arg)) {
                    return ObjectUtil.entity2map(arg, HashMap::new);
                }
            }
        }

        Map<String, Object> myArgs = new HashMap<>();
        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            Annotation[] annotations = parameter.getDeclaredAnnotations();
            if (annotations.length == 0) {
                throw new IllegalArgumentException(parameter.getName() + " has no @" + Arg.class.getSimpleName());
            }
            for (Annotation annotation : annotations) {
                if (annotation instanceof Arg) {
                    String argName = ((Arg) annotation).value();
                    myArgs.put(argName, args[i]);
                    break;
                }
            }
        }
        return myArgs;
    }
}