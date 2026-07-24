package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.TiFunction;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.common.util.ReflectUtils;
import com.github.chengyuxing.sql.annotation.*;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.plugins.*;
import com.github.chengyuxing.sql.support.BatchResult;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.annotation.SqlStatementType;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * XQL mapping to interface method invocation handler.
 */
public abstract class XQLInvocationHandler implements InvocationHandler {
    public static final Pattern QUERY_PATTERN = Pattern.compile("^(?:select|query|find|get|fetch|search|list)[^a-z]\\w*");
    public static final Pattern INSERT_PATTERN = Pattern.compile("^(?:insert|save|add|append|create)[^a-z]\\w*");
    public static final Pattern UPDATE_PATTERN = Pattern.compile("^(?:update|modify|change)[^a-z]\\w*");
    public static final Pattern DELETE_PATTERN = Pattern.compile("^(?:delete|remove)[^a-z]\\w*");
    public static final Pattern CALL_PATTERN = Pattern.compile("^(?:call|proc|func)[^a-z]\\w*");
    public static final Pattern BATCH_PATTERN = Pattern.compile("^batch[^a-z]\\w*");

    private final ClassLoader classLoader = this.getClass().getClassLoader();

    protected abstract @NotNull BakiDao baki();

    protected EntityManager.EntityMetaProvider entityMetaProvider() {
        return baki().getEntityManager().getEntityMetaProvider();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?> clazz = method.getDeclaringClass();
        Class<?> returnType = method.getReturnType();
        Class<?> returnGenericType = getReturnGenericType(method);

        final BakiDao baki = baki();

        Object myArgs = resolveArgs(method, args);

        if (method.isAnnotationPresent(Procedure.class)) {
            Procedure procedure = method.getDeclaredAnnotation(Procedure.class);
            return handleProcedure(baki, procedure.value(), myArgs, method, returnType);
        }

        if (method.isAnnotationPresent(com.github.chengyuxing.sql.annotation.Function.class)) {
            com.github.chengyuxing.sql.annotation.Function function = method.getDeclaredAnnotation(com.github.chengyuxing.sql.annotation.Function.class);
            return handleProcedure(baki, function.value(), myArgs, method, returnType);
        }

        String alias = clazz.getDeclaredAnnotation(XQLMapper.class).value();
        String sqlName = method.getName();

        SqlStatementType sqlType = null;

        if (method.isAnnotationPresent(XQL.class)) {
            XQL xql = method.getDeclaredAnnotation(XQL.class);
            if (!StringUtils.isBlank(xql.value())) {
                sqlName = xql.value();
            }
            sqlType = xql.type();
        }

        if (sqlType == null) {
            sqlType = detectSQLTypeByMethodPrefix(sqlName);
        }

        XQLFileManager.Resource xqlResource = baki.getXqlFileManager().getResource(alias);
        if (xqlResource == null) {
            throw new IllegalAccessException("XQL file alias '" + alias + "' not found at: " + clazz);
        }
        if (!xqlResource.getEntry().containsKey(sqlName)) {
            throw new IllegalAccessException("SQL name [" + sqlName + "] not found at: " + clazz + "#" + method.getName());
        }

        String sqlRef = "&" + XQLFileManager.encodeSqlReference(alias, sqlName);

        TiFunction<Baki, Method, Object[], Object> func = baki.getSqlInvokeHandler().invoke(sqlType);
        if (func != null) {
            return func.apply(baki, method, args);
        }

        switch (sqlType) {
            case query:
                return handleQuery(baki, alias, sqlName, myArgs, method, returnType, returnGenericType);
            case insert:
            case update:
            case delete:
            case dml:
                return handleModify(baki, sqlRef, myArgs, method, returnType);
            case batch:
                return handleBatchModify(baki, sqlRef, myArgs, method, returnType);
            case procedure:
            case function:
                return handleProcedure(baki, sqlRef, myArgs, method, returnType);
            case ddl:
            case plsql:
            case unset:
                return handleExecute(baki, sqlRef, myArgs, method, returnType);
            default:
                throw new IllegalAccessException(method.getDeclaringClass() + "#" + method.getName() + " SQL type [" + sqlType + "] not supported");
        }
    }

    protected SqlStatementType detectSQLTypeByMethodPrefix(String method) {
        if (QUERY_PATTERN.matcher(method).matches()) {
            return SqlStatementType.query;
        }
        if (INSERT_PATTERN.matcher(method).matches()) {
            return SqlStatementType.insert;
        }
        if (UPDATE_PATTERN.matcher(method).matches()) {
            return SqlStatementType.update;
        }
        if (DELETE_PATTERN.matcher(method).matches()) {
            return SqlStatementType.delete;
        }
        if (BATCH_PATTERN.matcher(method).matches()) {
            return SqlStatementType.batch;
        }
        if (CALL_PATTERN.matcher(method).matches()) {
            return SqlStatementType.procedure;
        }
        return SqlStatementType.unset;
    }

    protected DataRow handleExecute(BakiDao baki, String sqlRef, Object args, Method method, Class<?> returnType) {
        if (!Map.class.isAssignableFrom(returnType)) {
            throw new IllegalStateException(method.getDeclaringClass() + "#" + method.getName() + " return type must be Map or DataRow for the current XQL type");
        }
        //noinspection unchecked
        return baki.execute(sqlRef, (Map<String, ?>) args);
    }

    protected Object handleModify(BakiDao baki, String sqlRef, Object args, Method method, Class<?> returnType) {
        if (returnType == Integer.class || returnType == int.class) {
            //noinspection unchecked
            return baki.execute(sqlRef, (Map<String, ?>) args).getInt(0);
        }
        if (Map.class.isAssignableFrom(returnType)) {
            //noinspection unchecked
            return baki.execute(sqlRef, (Map<String, ?>) args);
        }
        throw new IllegalStateException(method.getDeclaringClass() + "#" + method.getName() + " return type must be Integer, int, Map or DataRow for the current XQL type");
    }

    protected Object handleBatchModify(BakiDao baki, String sqlRef, Object args, Method method, Class<?> returnType) {
        if (returnType == BatchResult.class) {
            return baki.execute(sqlRef, (Iterable<?>) args, element -> {
                if (element instanceof Map<?, ?>) {
                    //noinspection unchecked
                    return (Map<String, ?>) element;
                } else if (isBindableObject(element.getClass())) {
                    //noinspection unchecked
                    return (Map<String, ?>) entityArgToMap(element);
                } else {
                    throw unsupportedArg(method, element);
                }
            });
        }
        throw new IllegalStateException(method.getDeclaringClass() + "#" + method.getName() + " return type must be BatchResult for the current XQL type");
    }

    protected DataRow handleProcedure(BakiDao baki, String sqlRef, Object args, Method method, Class<?> returnType) {
        if (!Map.class.isAssignableFrom(returnType)) {
            throw new IllegalStateException(method.getDeclaringClass() + "#" + method.getName() + " return type must be Map or DataRow for the current XQL type");
        }
        Map<String, Param> myPaArgs = new HashMap<>();
        //noinspection unchecked
        for (Map.Entry<String, ?> entry : ((Map<String, ?>) args).entrySet()) {
            myPaArgs.put(entry.getKey(), (Param) entry.getValue());
        }
        return baki.call(sqlRef, myPaArgs);
    }

    protected Object handleQuery(BakiDao baki, String alias, String sqlName, Object args, Method method, Class<?> returnType, Class<?> genericType) {
        @SuppressWarnings("unchecked") QueryExecutor qe = baki.query("&" + XQLFileManager.encodeSqlReference(alias, sqlName)).args((Map<String, Object>) args);
        if (returnType == Stream.class) {
            return qe.stream().map(dataRowReturnTypeMapping(genericType));
        }
        if (returnType == List.class) {
            try (Stream<DataRow> s = qe.stream()) {
                return s.map(dataRowReturnTypeMapping(genericType)).collect(Collectors.toList());
            }
        }
        if (returnType == Set.class) {
            try (Stream<DataRow> s = qe.stream()) {
                return s.map(dataRowReturnTypeMapping(genericType)).collect(Collectors.toSet());
            }
        }
        if (returnType == String.class) {
            return qe.findFirstRow().getString(0);
        }
        if (returnType == boolean.class || returnType == Boolean.class) {
            Object first = qe.findFirstRow().getFirst(0);
            if (first != null) {
                if (first instanceof Boolean) {
                    return first;
                }
                String sv = first.toString();
                if (sv.equalsIgnoreCase("true") || sv.equalsIgnoreCase("false")) {
                    return Boolean.parseBoolean(sv);
                }
                if (sv.equals("0")) {
                    return false;
                }
                if (sv.equals("1")) {
                    return true;
                }
            }
            return returnType == Boolean.class ? null : false;
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
            return qe.findFirst().map(dataRowReturnTypeMapping(genericType));
        }
        if (returnType == IPageable.class) {
            return configurePageable(alias, qe, method);
        }
        if (returnType == PagedResource.class) {
            return configurePageable(alias, qe, method).collect(dataRowReturnTypeMapping(genericType));
        }
        if (isBindableObject(returnType)) {
            return qe.findFirstEntity(returnType);
        }
        throw new UnsupportedOperationException(method.getDeclaringClass() + "#" + method.getName() + ", unsupported return type: " + returnType.getName() + " for the current XQL type");
    }

    protected IPageable configurePageable(String alias, QueryExecutor qe, Method method) {
        IPageable pageable = qe.pageable();
        String count = null;
        if (method.isAnnotationPresent(CountQuery.class)) {
            CountQuery countQuery = method.getDeclaredAnnotation(CountQuery.class);
            count = "&" + XQLFileManager.encodeSqlReference(alias, countQuery.value());
            pageable.count(count);
        }
        if (method.isAnnotationPresent(PageableConfig.class)) {
            PageableConfig pageableConfig = method.getDeclaredAnnotation(PageableConfig.class);
            String[] startEnd = pageableConfig.disableDefaultPageSql();
            if (startEnd.length != 2) {
                throw new IllegalArgumentException(method.getDeclaringClass() + "#" + method.getName() + " @" + PageableConfig.class.getSimpleName() + ": it takes two key names for [start] and [end] number to overwrite");
            }
            Class<? extends PageHelperProvider> pageHelpProviderCls = pageableConfig.pageHelper();
            if (count == null) {
                throw new IllegalStateException(method.getDeclaringClass() + "#" + method.getName() + " has no @" + CountQuery.class.getSimpleName() + ", property disableDefaultPageSql must work with @" + CountQuery.class.getSimpleName());
            }
            pageable.disableDefaultPageSql(count, startEnd[0], startEnd[1]);
            if (!pageHelpProviderCls.getName().equals(PageHelperProvider.class.getName())) {
                try {
                    pageable.pageHelper(ReflectUtils.getInstance(pageHelpProviderCls));
                } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
                         InvocationTargetException e) {
                    throw new IllegalArgumentException(method.getDeclaringClass() + "#" + method.getName(), e);
                }
            }
        }
        return pageable;
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
        Type genericReturnType = method.getGenericReturnType();
        if (genericReturnType instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) genericReturnType).getActualTypeArguments();
            if (actualTypeArguments.length == 1) {
                Type actualTypeArgument = actualTypeArguments[0];
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
    protected Function<DataRow, Object> dataRowReturnTypeMapping(Class<?> genericType) {
        return d -> {
            if (genericType.isAssignableFrom(d.getClass())) {
                return d;
            }
            return d.toEntity(genericType,
                    field -> entityMetaProvider().columnMeta(field).getName(),
                    (field, value) -> entityMetaProvider().columnValue(field, value)
            );
        };
    }

    /**
     * Entity mapping to Map.
     *
     * @param entity entity
     * @return map
     */
    protected Object entityArgToMap(Object entity) {
        return ValueUtils.entityToMap(entity,
                f -> entityMetaProvider().columnMeta(f).getName(),
                HashMap::new
        );
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
        if (parameters.length == 1 && isImplicitSingleArg(parameters[0])) {
            return resolveSingleArg(method, args[0]);
        }
        return resolveNamedArgs(method, parameters, args);
    }

    private Object resolveSingleArg(Method method, Object arg) {
        if (arg == null) {
            return null;
        }
        if (arg instanceof Map<?, ?>) {
            return arg;
        }
        if (arg instanceof Iterable<?>) {
            return arg;
        }
        if (isBindableObject(arg.getClass())) {
            return entityArgToMap(arg);
        }
        throw unsupportedArg(method, arg);
    }

    private Object resolveNamedArgs(Method method, Parameter[] parameters, Object[] args) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < parameters.length; i++) {
            Arg arg = parameters[i].getAnnotation(Arg.class);
            if (arg == null) {
                throw new IllegalArgumentException(method.getDeclaringClass()
                        + "#" + method.getName()
                        + "#" + parameters[i].getName()
                        + " has no @" + Arg.class.getSimpleName());
            }
            result.put(arg.value(), args[i]);
        }
        return result;
    }

    private boolean isBindableObject(@NotNull Class<?> type) {
        String name = type.getName();
        if (StringUtils.startsWiths(name, "java.", "javax.", "jakarta.")) {
            return false;
        }
        if (type.isInterface() || type.isEnum() || type.isArray() || type.isPrimitive()) {
            return false;
        }
        if (type.isSynthetic() || StringUtils.startsWiths(name, "$$", "$Proxy")) {
            return false;
        }
        for (Field field : type.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    private boolean isImplicitSingleArg(Parameter parameter) {
        return parameter.getAnnotation(Arg.class) == null;
    }

    private IllegalArgumentException unsupportedArg(Method method, Object element) {
        return new IllegalArgumentException(method.getDeclaringClass()
                + "#" + method.getName()
                + "#" + element.getClass().getSimpleName());
    }
}