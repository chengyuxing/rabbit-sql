package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.anno.Alias;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.anno.Arg;
import com.github.chengyuxing.sql.anno.XQL;
import com.github.chengyuxing.sql.anno.XQLMapper;
import com.github.chengyuxing.sql.datasource.DataSourceUtil;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.IllegalSqlException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.PageHelperProvider;
import com.github.chengyuxing.sql.page.impl.Db2PageHelper;
import com.github.chengyuxing.sql.page.impl.MysqlPageHelper;
import com.github.chengyuxing.sql.page.impl.OraclePageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.support.*;
import com.github.chengyuxing.sql.support.executor.EntitySaveExecutor;
import com.github.chengyuxing.sql.support.executor.Executor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.SaveExecutor;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <h2>Default implementation of Baki interface</h2>
 * <p>If {@link XQLFileManager } configured, all methods will be support replace sql statement to sql name ({@code &<alias>.<sqlName>}).</p>
 * <p>Example:</p>
 * <blockquote><pre>
 * try ({@link Stream}&lt;{@link DataRow}&gt; s = baki.query("&amp;sys.getUser").stream()) {
 *     s.forEach(System.out::println);
 * }</pre></blockquote>
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final DataSource dataSource;
    private XQLInvocationHandler xqlInvocationHandler;
    private DatabaseMetaData metaData;
    private String databaseId;
    private SqlGenerator sqlGenerator;
    //---------optional properties------
    /**
     * Global custom page helper provider.
     */
    private PageHelperProvider globalPageHelperProvider;
    /**
     * Custom sql interceptor.
     */
    private SqlInterceptor sqlInterceptor;
    /**
     * Custom prepared sql statement parameter value handler.
     */
    private StatementValueHandler statementValueHandler;
    /**
     * Do something after parse dynamic sql.
     */
    private AfterParseDynamicSql afterParseDynamicSql;
    /**
     * Sql watcher.
     */
    private SqlWatcher sqlWatcher;
    /**
     * XQL file manager.
     */
    private XQLFileManager xqlFileManager;
    /**
     * Batch size for execute batch.
     */
    private int batchSize = 1000;
    /**
     * Named parameter prefix symbol.
     */
    private char namedParamPrefix = ':';
    /**
     * If XQL file changed, XQL file will reload when {@link #parseSql(String, Map)} invoke always.
     */
    private boolean reloadXqlOnGet = false;
    /**
     * Load {@code xql-file-manager-}{@link #databaseId() databaseId}{@code .yml} first if exists,
     * otherwise {@code xql-file-manager.yml}
     */
    private boolean autoXFMConfig = false;
    /**
     * Page query page number argument key.
     */
    private String pageKey = "page";
    /**
     * Page query page size argument key.
     */
    private String sizeKey = "size";
    /**
     * Jdbc execute sql timeout({@link Statement#setQueryTimeout(int)}) handler.
     */
    private QueryTimeoutHandler queryTimeoutHandler;

    /**
     * Constructs a new BakiDao with initial datasource.
     *
     * @param dataSource datasource
     */
    public BakiDao(DataSource dataSource) {
        this.dataSource = dataSource;
        init();
    }

    /**
     * Initialize default configuration properties.
     */
    protected void init() {
        this.xqlInvocationHandler = new XQLInvocationHandler();
        this.sqlGenerator = new SqlGenerator(namedParamPrefix);
        this.statementValueHandler = (ps, index, value, metaData) -> JdbcUtil.setStatementValue(ps, index, value);
        this.afterParseDynamicSql = sql -> sql;
        this.sqlWatcher = (sourceSql, targetSql, args, startTime, endTime, throwable) -> {
        };
        this.queryTimeoutHandler = (sql, args) -> 0;
        using(c -> {
            try {
                this.metaData = c.getMetaData();
                this.databaseId = this.metaData.getDatabaseProductName().toLowerCase();
                return 0;
            } catch (SQLException e) {
                throw new UncheckedSqlException("initialize metadata error.", e);
            }
        });
    }

    /**
     * Returns the mapper interface proxy instance.
     *
     * @param clazz mapper interface
     * @param <T>   interface type
     * @return interface instance
     * @throws IllegalAccessException not interface or has no @XQLMapper
     */
    @SuppressWarnings("unchecked")
    public <T> T registerXQLMapper(Class<T> clazz) throws IllegalAccessException {
        if (!clazz.isInterface()) {
            throw new IllegalAccessException("Not interface: " + clazz.getName());
        }
        if (!clazz.isAnnotationPresent(XQLMapper.class)) {
            throw new IllegalAccessException(clazz + " should be annotated with @" + XQLMapper.class.getSimpleName());
        }
        return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{clazz}, xqlInvocationHandler);
    }

    @Override
    public QueryExecutor query(String sql) {
        return new QueryExecutor(sql) {
            @Override
            public Stream<DataRow> stream() {
                return watchSql(sql, sql, args, () -> executeQueryStream(sql, args));
            }

            @Override
            public List<Map<String, Object>> maps() {
                try (Stream<DataRow> s = stream()) {
                    return s.collect(Collectors.toList());
                }
            }

            @Override
            public List<DataRow> rows() {
                try (Stream<DataRow> s = stream()) {
                    return s.collect(Collectors.toList());
                }
            }

            @Override
            public <T> List<T> entities(Class<T> entityClass) {
                try (Stream<DataRow> s = stream()) {
                    return s.map(d -> d.toEntity(entityClass)).collect(Collectors.toList());
                }
            }

            @Override
            public DataRow zip() {
                return DataRow.zip(rows());
            }

            @Override
            public IPageable pageable(int page, int size) {
                IPageable iPageable = new SimplePageable(sql, page, size);
                return iPageable.args(args);
            }

            @Override
            public IPageable pageable(String pageKey, String sizeKey) {
                Integer page = (Integer) args.get(pageKey);
                Integer size = (Integer) args.get(sizeKey);
                if (page == null || size == null) {
                    throw new IllegalArgumentException("page or size is null");
                }
                return pageable(page, size);
            }

            /**
             * {@inheritDoc}
             * <ul>
             *     <li>Default page number key: {@link BakiDao#getPageKey()}</li>
             *     <li>Default page size key: {@link BakiDao#getSizeKey()}</li>
             * </ul>
             * @return IPageable instance
             */
            @Override
            public IPageable pageable() {
                return pageable(pageKey, sizeKey);
            }

            @Override
            public DataRow findFirstRow() {
                return findFirst().orElseGet(() -> new DataRow(0));
            }

            @Override
            public <T> T findFirstEntity(Class<T> entityClass) {
                return findFirst().map(d -> d.toEntity(entityClass)).orElse(null);
            }

            @Override
            public Optional<DataRow> findFirst() {
                return findFirst(true);
            }

            @Override
            public Optional<DataRow> findFirst(boolean pageQuery1st) {
                if (pageQuery1st) {
                    PageHelper pageHelper = defaultPager();
                    pageHelper.init(1, 1, 1);
                    Map<String, Integer> pagedArgs = pageHelper.pagedArgs();
                    if (Objects.nonNull(pagedArgs)) {
                        args.putAll(pagedArgs);
                    }
                    Pair<String, Map<String, Object>> result = parseSql(sql, args);
                    String pagedSql = pageHelper.pagedSql(result.getItem1());

                    return watchSql(sql, pagedSql, args, () -> {
                        try (Stream<DataRow> s = executeQueryStream(pagedSql, result.getItem2())) {
                            return s.peek(d -> d.remove(PageHelper.ROW_NUM_KEY)).findFirst();
                        }
                    });
                }
                try (Stream<DataRow> s = stream()) {
                    return s.findFirst();
                }
            }

            @Override
            public boolean exists() {
                return findFirst().isPresent();
            }
        };
    }

    @Override
    public <T> SaveExecutor<T> update(String tableName, String where) {
        return new SaveExecutor<T>() {
            @Override
            public int save(Map<String, ?> data) {
                if (data.isEmpty()) {
                    return 0;
                }
                String whereStatement = parseSql(where, Collections.emptyMap()).getItem1();
                List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
                String update = sqlGenerator.generateNamedParamUpdate(tableName,
                        whereStatement,
                        data,
                        tableFields,
                        ignoreNull);
                return watchSql(update, update, data, () -> executeUpdate(update, data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                if (data.isEmpty()) {
                    return 0;
                }
                String whereStatement = parseSql(where, Collections.emptyMap()).getItem1();
                List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
                String update = sqlGenerator.generateNamedParamUpdate(tableName,
                        whereStatement,
                        data.iterator().next(),
                        tableFields,
                        ignoreNull);
                return watchSql(update, update, data, () -> executeBatchUpdate(update, data, batchSize));
            }
        };
    }

    @Override
    public <T> SaveExecutor<T> insert(String tableName) {
        return new SaveExecutor<T>() {
            @Override
            public int save(Map<String, ?> data) {
                if (data.isEmpty()) {
                    return 0;
                }
                List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
                String insert = sqlGenerator.generateNamedParamInsert(tableName, data, tableFields, ignoreNull);
                return watchSql(insert, insert, data, () -> executeUpdate(insert, data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                if (data.isEmpty()) {
                    return 0;
                }
                List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
                String insert = sqlGenerator.generateNamedParamInsert(tableName, data.iterator().next(), tableFields, ignoreNull);
                return watchSql(insert, insert, data, () -> executeBatchUpdate(insert, data, batchSize));
            }
        };
    }

    @Override
    public <T> SaveExecutor<T> delete(String tableName, String where) {
        String whereSql = parseSql(where, Collections.emptyMap()).getItem1();
        String w = StringUtil.startsWithIgnoreCase(whereSql.trim(), "where") ? whereSql : "\nwhere " + whereSql;
        String delete = "delete from " + tableName + w;

        return new SaveExecutor<T>() {

            @Override
            public int save(Map<String, ?> data) {
                return watchSql(delete, delete, data, () -> executeUpdate(delete, data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                return watchSql(delete, delete, data, () -> executeBatchUpdate(delete, data, batchSize));
            }
        };
    }

    @Override
    public <T> EntitySaveExecutor<T> entity(Class<T> entityClass) {
        String tableName = getTableNameByAlias(entityClass);
        return new EntitySaveExecutor<T>() {
            @Override
            public SaveExecutor<T> insert() {
                return BakiDao.this.insert(tableName);
            }

            @Override
            public SaveExecutor<T> update(String where) {
                return BakiDao.this.update(tableName, where);
            }

            @Override
            public SaveExecutor<T> delete(String where) {
                return BakiDao.this.delete(tableName, where);
            }
        };
    }

    @Override
    public Executor of(String sql) {
        return new Executor() {
            @Override
            public DataRow execute() {
                return watchSql(sql, sql, Collections.emptyMap(), () -> BakiDao.super.execute(sql, Collections.emptyMap()));
            }

            @Override
            public DataRow execute(Map<String, ?> args) {
                return watchSql(sql, sql, args, () -> BakiDao.super.execute(sql, args));
            }

            @Override
            public int executeBatch(String... moreSql) {
                List<String> sqlList = new ArrayList<>(Arrays.asList(moreSql));
                sqlList.add(0, sql);
                String s = String.join("###", sqlList);
                return watchSql(s, s, Collections.emptyMap(), () -> BakiDao.super.executeBatch(sqlList, batchSize));
            }

            @Override
            public int executeBatch(List<String> moreSql) {
                String s = String.join("###", moreSql);
                return watchSql(s, s, Collections.emptyMap(), () -> BakiDao.super.executeBatch(moreSql, batchSize));
            }

            @Override
            public int executeBatch(Collection<? extends Map<String, ?>> data) {
                Map<String, ?> arg = data.isEmpty() ? new HashMap<>() : data.iterator().next();
                Pair<String, Map<String, Object>> parsed = parseSql(sql, arg);
                return watchSql(sql, parsed.getItem1(), data, () -> {
                    Collection<? extends Map<String, ?>> newData;
                    if (parsed.getItem2().containsKey(XQLFileManager.DynamicSqlParser.FOR_VARS_KEY) &&
                            parsed.getItem1().contains(XQLFileManager.DynamicSqlParser.VAR_PREFIX)) {
                        List<Map<String, Object>> list = new ArrayList<>();
                        for (Map<String, ?> item : data) {
                            list.add(parseSql(sql, item).getItem2());
                        }
                        newData = list;
                    } else {
                        newData = data;
                    }
                    return executeBatchUpdate(parsed.getItem1(), newData, batchSize);
                });

            }

            @Override
            public DataRow call(Map<String, Param> params) {
                return watchSql(sql, sql, params, () -> executeCallStatement(sql, params));
            }
        };
    }

    /**
     * Watch sql execution status.
     *
     * @param sourceSql source sql
     * @param targetSql target sql
     * @param args      args
     * @param supplier  supplier
     * @param <T>       type
     * @return any
     */
    protected <T> T watchSql(String sourceSql, String targetSql, Object args, Supplier<T> supplier) {
        long startTime = System.currentTimeMillis();
        Throwable throwable = null;
        try {
            return supplier.get();
        } catch (Exception e) {
            throwable = e;
            throw e;
        } finally {
            sqlWatcher.watch(sourceSql, targetSql, args, startTime, System.currentTimeMillis(), throwable);
        }
    }

    @Override
    public <T> T using(Function<Connection, T> func) {
        Connection connection = null;
        try {
            connection = getConnection();
            return func.apply(connection);
        } finally {
            releaseConnection(connection, getDataSource());
        }
    }

    @Override
    public DatabaseMetaData metaData() {
        return this.metaData;
    }

    @Override
    public String databaseId() {
        return this.databaseId;
    }

    public String getPageKey() {
        return pageKey;
    }

    public void setPageKey(String pageKey) {
        this.pageKey = pageKey;
    }

    public String getSizeKey() {
        return sizeKey;
    }

    public void setSizeKey(String sizeKey) {
        this.sizeKey = sizeKey;
    }

    public void setSqlWatcher(SqlWatcher sqlWatcher) {
        if (Objects.nonNull(sqlWatcher)) {
            this.sqlWatcher = sqlWatcher;
        }
    }

    public void setQueryTimeoutHandler(QueryTimeoutHandler queryTimeoutHandler) {
        if (Objects.nonNull(queryTimeoutHandler)) {
            this.queryTimeoutHandler = queryTimeoutHandler;
        }
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    public class XQLInvocationHandler implements InvocationHandler {
        private final ClassLoader classLoader = this.getClass().getClassLoader();
        private final Map<XQL.Type, SqlInvokeHandler> handlers = new HashMap<>();

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Class<?> clazz = method.getDeclaringClass();
            String xqlFileAlias = clazz.getDeclaredAnnotation(XQLMapper.class).value();
            XQL.Type sqlType = XQL.Type.QUERY;
            String sqlName = method.getName();

            if (method.isAnnotationPresent(XQL.class)) {
                XQL annotation = method.getDeclaredAnnotation(XQL.class);
                if (!annotation.value().trim().isEmpty()) {
                    sqlName = annotation.value();
                }
                sqlType = annotation.type();
            }
            XQLFileManager.Resource xqlResource = getXqlFileManager().getResource(xqlFileAlias);
            if (xqlResource == null) {
                throw new IllegalAccessException("XQL file alias '" + xqlFileAlias + "' not found at: " + clazz.getName());
            }
            if (!xqlResource.getEntry().containsKey(sqlName)) {
                throw new IllegalAccessException("SQL name [" + sqlName + "] not found at: " + clazz.getName() + "#" + method.getName());
            }
            String sqlRef = "&" + xqlFileAlias + "." + sqlName;
            return doInvoke(sqlType, sqlRef, args, method);
        }

        /**
         * Do invoke.
         *
         * @param sqlType sql type
         * @param sqlRef  sql reference
         * @param args    args
         * @param method  method
         * @return any
         * @throws Throwable throwable
         */
        protected Object doInvoke(XQL.Type sqlType, String sqlRef, Object[] args, Method method) throws Throwable {
            Class<?> returnType = method.getReturnType();
            Class<?> returnGenericType = getReturnGenericType(method);
            Object myArgs = resolveArgs(method, args);
            if (handlers.containsKey(sqlType)) {
                SqlInvokeHandler handler = handlers.get(sqlType);
                return handler.handle(BakiDao.this, sqlRef, myArgs, method, returnType, returnGenericType);
            }
            switch (sqlType) {
                case QUERY:
                    return handleQuery(sqlRef, myArgs, method, returnType, returnGenericType);
                case INSERT:
                case UPDATE:
                case DELETE:
                case DDL:
                    return handleModify(sqlRef, myArgs, method, returnType);
                case PROCEDURE:
                case FUNCTION:
                    return handleProcedure(sqlRef, myArgs, method, returnType);
                case PLSQL:
                    return handlePlsql(sqlRef, myArgs, method, returnType);
                default:
                    return null;
            }
        }

        public void addHandler(XQL.Type sqlType, SqlInvokeHandler handler) {
            handlers.put(sqlType, handler);
        }

        protected DataRow handlePlsql(String sqlRef, Object args, Method method, Class<?> returnType) {
            if (!Map.class.isAssignableFrom(returnType)) {
                throw new IllegalStateException(method.getName() + " return type must be Map");
            }
            //noinspection unchecked
            return of(sqlRef).execute((Map<String, Object>) args);
        }

        protected int handleModify(String sqlRef, Object args, Method method, Class<?> returnType) {
            if (returnType != Integer.class && returnType != int.class) {
                throw new IllegalStateException(method.getName() + " return type must be Integer or int");
            }
            if (args instanceof Map) {
                //noinspection unchecked
                return of(sqlRef).execute((Map<String, Object>) args).getFirstAs();
            }
            if (args instanceof Collection) {
                List<Map<String, Object>> myArgsList = new ArrayList<>();
                for (Object arg : (Collection<?>) args) {
                    if (arg instanceof Map) {
                        //noinspection unchecked
                        myArgsList.add((Map<String, Object>) arg);
                        continue;
                    }
                    if (!arg.getClass().getName().startsWith("java.")) {
                        myArgsList.add(ObjectUtil.entity2map(arg, HashMap::new));
                        continue;
                    }
                    throw new IllegalArgumentException(method.getName() + " unsupported arg type: " + arg.getClass().getName());
                }
                return of(sqlRef).executeBatch(myArgsList);
            }
            throw new IllegalArgumentException(method.getName() + " args must be Map or Collection");
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
            return of(sqlRef).call(myPaArgs);
        }

        protected Object handleQuery(String sqlRef, Object args, Method method, Class<?> returnType, Class<?> genericType) throws Throwable {
            if (args instanceof Collection) {
                throw new IllegalArgumentException(method.getName() + " args must not be Collection");
            }
            @SuppressWarnings("unchecked") QueryExecutor qe = query(sqlRef).args((Map<String, Object>) args);
            if (returnType == Stream.class) {
                return qe.stream().map(dataRowMapping(genericType));
            }
            if (returnType == List.class) {
                try (Stream<DataRow> s = qe.stream()) {
                    return s.map(dataRowMapping(genericType)).collect(Collectors.toList());
                }
            }
            if (Map.class.isAssignableFrom(returnType)) {
                return qe.findFirstRow();
            }
            if (returnType == Optional.class) {
                return qe.findFirst().map(dataRowMapping(genericType));
            }
            if (returnType == IPageable.class) {
                return qe.pageable();
            }
            if (returnType == PagedResource.class) {
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
                    if (arg instanceof Map || arg instanceof Collection) {
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
                Annotation[] annotations = parameter.getAnnotations();
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

    /**
     * Simple page helper implementation.
     */
    class SimplePageable extends IPageable {

        /**
         * Constructs a SimplePageable.
         *
         * @param recordQuery record query statement
         * @param page        current page
         * @param size        page size
         */
        public SimplePageable(String recordQuery, int page, int size) {
            super(recordQuery, page, size);
        }

        @Override
        public <T> PagedResource<T> collect(Function<DataRow, T> mapper) {
            Pair<String, Map<String, Object>> result = parseSql(recordQuery, args);
            String query = result.getItem1();
            Map<String, Object> myArgs = result.getItem2();
            if (count == null) {
                String cq = countQuery == null ? sqlGenerator.generateCountQuery(query) : countQuery;
                List<DataRow> cnRows = watchSql(recordQuery, cq, myArgs, () -> execute(cq, myArgs)).getFirstAs();
                Object cn = cnRows.get(0).getFirst();
                if (cn instanceof Integer) {
                    count = (Integer) cn;
                } else {
                    count = Integer.parseInt(cn.toString());
                }
            }

            PageHelper pageHelper = null;

            if (pageHelperProvider != null) {
                pageHelper = pageHelperProvider.customPageHelper(metaData, databaseId, namedParamPrefix);
            }

            if (pageHelper == null) {
                pageHelper = defaultPager();
            }
            pageHelper.init(page, size, count);
            Args<Integer> pagedArgs = pageHelper.pagedArgs();
            if (pagedArgs == null) {
                pagedArgs = Args.of();
            }
            myArgs.putAll(rewriteArgsFunc == null ? pagedArgs : rewriteArgsFunc.apply(pagedArgs));
            String executeQuery = disablePageSql ? query : pageHelper.pagedSql(query);
            final PageHelper finalPageHelper = pageHelper;
            return watchSql(recordQuery, executeQuery, myArgs, () -> {
                try (Stream<DataRow> s = executeQueryStream(executeQuery, myArgs)) {
                    List<T> list = s.peek(d -> d.remove(PageHelper.ROW_NUM_KEY))
                            .map(mapper)
                            .collect(Collectors.toList());
                    return PagedResource.of(finalPageHelper, list);
                }
            });
        }
    }

    /**
     * Built-in default page helper.
     *
     * @return PageHelper instance
     * @throws UnsupportedOperationException there is no default implementation of your database
     * @throws ConnectionStatusException     connection status exception
     */
    protected PageHelper defaultPager() {
        if (Objects.nonNull(globalPageHelperProvider)) {
            PageHelper pageHelper = globalPageHelperProvider.customPageHelper(metaData, databaseId, namedParamPrefix);
            if (Objects.nonNull(pageHelper)) {
                return pageHelper;
            }
        }
        switch (databaseId) {
            case "oracle":
                return new OraclePageHelper();
            case "postgresql":
            case "sqlite":
                return new PGPageHelper();
            case "mysql":
            case "mariadb":
                return new MysqlPageHelper();
            case "z/os":
            case "sqlds":
            case "iseries":
            case "db2 for unix/windows":
            case "cloudscape":
            case "informix":
                return new Db2PageHelper();
            default:
                throw new UnsupportedOperationException("pager of \"" + databaseId + "\" default not implement currently, see method 'setGlobalPageHelperProvider'.");
        }
    }

    /**
     * Get table name by {@link Alias @Alias} .
     *
     * @param entityClass entity class
     * @return table name
     */
    protected String getTableNameByAlias(Class<?> entityClass) {
        if (entityClass == null) {
            throw new IllegalArgumentException("entityClass must not be null.");
        }
        String tableName = entityClass.getSimpleName();
        if (entityClass.isAnnotationPresent(Alias.class)) {
            tableName = entityClass.getAnnotation(Alias.class).value();
        }
        return tableName;
    }

    /**
     * Get all fields from target table.
     *
     * @param tableName table name
     * @return table fields
     * @throws UncheckedSqlException query exception
     */
    protected List<String> getTableFields(String tableName) {
        String sql = parseSql("select * from " + tableName + " where 1 = 2", Collections.emptyMap()).getItem1();
        return execute(sql, sc -> {
            ResultSet fieldsResultSet = sc.executeQuery();
            List<String> fields = Arrays.asList(JdbcUtil.createNames(fieldsResultSet, ""));
            JdbcUtil.closeResultSet(fieldsResultSet);
            return fields;
        });
    }

    /**
     * Reload xql file manager by database id if necessary.
     */
    protected void loadXFMConfigByDatabaseId() {
        if (Objects.nonNull(xqlFileManager)) {
            String pathByDb = "xql-file-manager-" + databaseId + ".yml";
            FileResource resource = new FileResource(pathByDb);
            if (!resource.exists()) {
                resource = new FileResource(XQLFileManager.YML);
            }
            if (resource.exists()) {
                XQLFileManagerConfig config = new XQLFileManagerConfig();
                config.loadYaml(resource);
                config.copyStateTo(xqlFileManager);
                xqlFileManager.init();
                log.debug("{} detected by '{}' and loaded!", resource.getFileName(), databaseId);
            }
        }
    }

    /**
     * Get sql from {@link XQLFileManager} by sql name if first arg starts with symbol ({@code &}).<br>
     * Sql name format: {@code &<alias>.<sqlName>}
     *
     * @param sql  sql statement or sql name
     * @param args args
     * @return sql
     * @throws NullPointerException if first arg starts with symbol ({@code &}) but {@link XQLFileManager} not configured
     * @throws IllegalSqlException  sql interceptor reject sql
     */
    @Override
    protected Pair<String, Map<String, Object>> parseSql(String sql, Map<String, ?> args) {
        Map<String, Object> myArgs = new HashMap<>();
        if (Objects.nonNull(args)) {
            myArgs.putAll(args);
        }
        String trimSql = SqlUtil.trimEnd(sql.trim());
        if (trimSql.startsWith("&")) {
            if (Objects.nonNull(xqlFileManager)) {
                if (reloadXqlOnGet) {
                    log.warn("please set 'reloadXqlOnGet' to false in production environment for improve concurrency.");
                    xqlFileManager.init();
                }
                Pair<String, Map<String, Object>> result = xqlFileManager.get(trimSql.substring(1), myArgs);
                trimSql = afterParseDynamicSql.handle(result.getItem1());
                // #for expression temp variables stored in _for variable.
                if (!result.getItem2().isEmpty()) {
                    myArgs.put(XQLFileManager.DynamicSqlParser.FOR_VARS_KEY, result.getItem2());
                }
            } else {
                throw new NullPointerException("can not find property 'xqlFileManager'.");
            }
        }
        if (trimSql.contains("${")) {
            trimSql = SqlUtil.formatSql(trimSql, myArgs, sqlGenerator.getTemplateFormatter());
            if (Objects.nonNull(xqlFileManager)) {
                trimSql = SqlUtil.formatSql(trimSql, xqlFileManager.getConstants(), sqlGenerator.getTemplateFormatter());
            }
        }
        if (Objects.nonNull(sqlInterceptor)) {
            boolean request = sqlInterceptor.preHandle(trimSql, myArgs, metaData);
            if (!request) {
                throw new IllegalSqlException("permission denied, reject to execute sql.\nSQL: " + trimSql + "\nArgs: " + myArgs);
            }
        }
        return Pair.of(trimSql, myArgs);
    }

    @Override
    protected SqlGenerator sqlGenerator() {
        return sqlGenerator;
    }

    @Override
    protected DataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected Connection getConnection() {
        try {
            return DataSourceUtil.getConnection(dataSource);
        } catch (SQLException e) {
            throw new ConnectionStatusException("fetch connection failed.", e);
        }
    }

    @Override
    protected void releaseConnection(Connection connection, DataSource dataSource) {
        DataSourceUtil.releaseConnection(connection, dataSource);
    }

    @Override
    protected void doHandleStatementValue(PreparedStatement ps, int index, Object value) throws SQLException {
        statementValueHandler.handle(ps, index, value, metaData);
    }

    @Override
    protected int queryTimeout(String sql, Map<String, ?> args) {
        return queryTimeoutHandler.handle(sql, args);
    }

    public XQLInvocationHandler getXqlInvocationHandler() {
        return xqlInvocationHandler;
    }

    public SqlGenerator getSqlGenerator() {
        return sqlGenerator;
    }

    public void setGlobalPageHelperProvider(PageHelperProvider globalPageHelperProvider) {
        this.globalPageHelperProvider = globalPageHelperProvider;
    }

    public void setSqlInterceptor(SqlInterceptor sqlInterceptor) {
        this.sqlInterceptor = sqlInterceptor;
    }

    public void setStatementValueHandler(StatementValueHandler statementValueHandler) {
        if (Objects.nonNull(statementValueHandler))
            this.statementValueHandler = statementValueHandler;
    }

    public void setAfterParseDynamicSql(AfterParseDynamicSql afterParseDynamicSql) {
        if (Objects.nonNull(afterParseDynamicSql))
            this.afterParseDynamicSql = afterParseDynamicSql;
    }

    public void setXqlFileManager(XQLFileManager xqlFileManager) {
        if (Objects.nonNull(xqlFileManager)) {
            this.xqlFileManager = xqlFileManager;
            this.xqlFileManager.setDatabaseId(databaseId);
            this.xqlFileManager.setTemplateFormatter(sqlGenerator.getTemplateFormatter());
            if (autoXFMConfig) {
                loadXFMConfigByDatabaseId();
                return;
            }
            if (!this.xqlFileManager.isInitialized()) {
                this.xqlFileManager.init();
            }
        }
    }

    public XQLFileManager getXqlFileManager() {
        return xqlFileManager;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }

    public void setNamedParamPrefix(char namedParamPrefix) {
        this.namedParamPrefix = namedParamPrefix;
        this.sqlGenerator = new SqlGenerator(this.namedParamPrefix);
    }

    public boolean isReloadXqlOnGet() {
        return reloadXqlOnGet;
    }

    public void setReloadXqlOnGet(boolean reloadXqlOnGet) {
        this.reloadXqlOnGet = reloadXqlOnGet;
    }

    public boolean isAutoXFMConfig() {
        return autoXFMConfig;
    }

    public void setAutoXFMConfig(boolean autoXFMConfig) {
        this.autoXFMConfig = autoXFMConfig;
        if (this.autoXFMConfig) {
            loadXFMConfigByDatabaseId();
        }
    }
}
