package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.AroundExecutor;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.datasource.DataSourceUtil;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.SqlRuntimeException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.*;
import com.github.chengyuxing.sql.plugins.*;
import com.github.chengyuxing.sql.support.*;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.annotation.SqlStatementType;
import com.github.chengyuxing.sql.types.Execution;
import com.github.chengyuxing.sql.utils.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
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
    private static final Logger log = LoggerFactory.getLogger(BakiDao.class);
    private static final String INTERNAL_PAGE_HELPER_ARG_KEY = "_$pageHelper";
    private static final String SQL_REF_MODIFIER_COUNT = "count";
    private static final String SQL_REF_MODIFIER_PAGE = "page";
    private final DataSource dataSource;
    private DatabaseMetaData metaData;
    private String databaseId;
    private SqlGenerator sqlGenerator;
    private AroundExecutor<Execution> sqlAroundExecutor;

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
     * Sql mapping interface invoke handler.
     */
    private SqlInvokeHandler sqlInvokeHandler;
    /**
     * Query cache manager.
     */
    private QueryCacheManager queryCacheManager;
    /**
     * Execution watchers.
     */
    private ExecutionWatcher executionWatcher;
    /**
     * Default Map to Entity field mapper support.
     */
    private EntityFieldMapper entityFieldMapper;
    /**
     * Default Map to Entity value mapper support.
     */
    private EntityValueMapper entityValueMapper;

    /**
     * Constructs a new BakiDao with initial datasource.
     *
     * @param dataSource datasource
     */
    public BakiDao(@NotNull DataSource dataSource) {
        this.dataSource = dataSource;
        init();
    }

    /**
     * Initialize default configuration properties.
     */
    protected void init() {
        this.sqlGenerator = new SqlGenerator(namedParamPrefix);
        this.sqlAroundExecutor = new AroundExecutor<Execution>() {
            @Override
            protected void onStart(@NotNull Execution identifier) {
                if (executionWatcher != null) executionWatcher.onStart(identifier);
            }

            @Override
            protected void onStop(@NotNull Execution identifier, @Nullable Object result, @Nullable Throwable throwable) {
                if (executionWatcher != null) executionWatcher.onStop(identifier, result, throwable);
            }
        };
        this.statementValueHandler = (ps, index, value, metaData) -> JdbcUtil.setStatementValue(ps, index, value);
        this.queryTimeoutHandler = (sql, args) -> 0;
        this.sqlInvokeHandler = type -> null;
        this.entityFieldMapper = Field::getName;
        this.entityValueMapper = null;
        this.using(c -> {
            try {
                this.metaData = c.getMetaData();
                this.databaseId = this.metaData.getDatabaseProductName().toLowerCase();
                return 0;
            } catch (SQLException e) {
                throw new UncheckedSqlException("Initialize metadata error.", e);
            }
        });
    }

    /**
     * Returns the mapper interface proxy instance.
     *
     * @param mapperInterface mapper interface
     * @param <T>             interface type
     * @return interface instance
     * @throws IllegalAccessException not interface or has no @XQLMapper
     */
    public <T> T proxyXQLMapper(@NotNull Class<T> mapperInterface) throws IllegalAccessException {
        return XQLMapperUtil.getProxyInstance(mapperInterface, new XQLInvocationHandler() {
            @Override
            protected @NotNull BakiDao baki() {
                return BakiDao.this;
            }
        });
    }

    @Override
    public DataRow executeAny(@NotNull String sql, Map<String, ?> args) {
        return this.sqlAroundExecutor.call(new Execution(SqlStatementType.unset, sql, args),
                i -> super.executeAny(sql, args));
    }

    @Override
    public Stream<DataRow> executeQueryStream(@NotNull String sql, Map<String, ?> args) {
        return this.sqlAroundExecutor.call(new Execution(SqlStatementType.query, sql, args),
                i -> {
                    if (Objects.isNull(queryCacheManager) || !queryCacheManager.isAvailable(sql, args)) {
                        return super.executeQueryStream(sql, args);
                    }
                    log.debug("The query({}, {}) has been taken over by the cache.", sql, args);
                    return queryCacheManager.get(sql, args, () -> super.executeQueryStream(sql, args));
                });
    }

    @Override
    public int executeUpdate(@NotNull String sql, Map<String, ?> args) {
        return this.sqlAroundExecutor.call(new Execution(SqlStatementType.dml, sql, args),
                i -> super.executeUpdate(sql, args));
    }

    @Override
    public <T> int executeBatchUpdate(@NotNull String sql,
                                      @NotNull Iterable<T> args,
                                      @NotNull Function<T, ? extends Map<String, ?>> eachMapper,
                                      @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        return this.sqlAroundExecutor.call(new Execution(SqlStatementType.dml, sql, args),
                i -> super.executeBatchUpdate(sql, args, eachMapper, batchSize));
    }

    @Override
    public DataRow executeCallStatement(@NotNull String procedure, Map<String, Param> args) {
        return this.sqlAroundExecutor.call(new Execution(SqlStatementType.procedure, procedure, args),
                i -> super.executeCallStatement(procedure, args));
    }

    @Override
    public int executeBatch(@NotNull Iterable<String> sqls, @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        return this.sqlAroundExecutor.call(new Execution(SqlStatementType.batch, String.join(";", sqls), null),
                i -> super.executeBatch(sqls, batchSize));
    }

    @Override
    public QueryExecutor query(@NotNull String sql) {
        return new QueryExecutor(sql) {
            @Override
            public Stream<DataRow> stream() {
                return executeQueryStream(sql, args);
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
                    return s.map(d -> d.toEntity(entityClass, entityFieldMapper::apply, entityValueMapper::apply))
                            .collect(Collectors.toList());
                }
            }

            @Override
            public IPageable pageable(int page, int size) {
                IPageable iPageable = new SimplePageable(sql, page, size);
                return iPageable.args(args);
            }

            @Override
            public IPageable pageable(@NotNull String pageKey, @NotNull String sizeKey) {
                Integer page = (Integer) args.get(pageKey);
                Integer size = (Integer) args.get(sizeKey);
                if (page == null || size == null) {
                    throw new IllegalArgumentException("Page or size is null.");
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
            public @NotNull DataRow findFirstRow() {
                return findFirst().orElseGet(() -> new DataRow(0));
            }

            @Override
            public <T> T findFirstEntity(Class<T> entityClass) {
                return findFirst()
                        .map(d -> d.toEntity(entityClass, entityFieldMapper::apply, entityValueMapper::apply))
                        .orElse(null);
            }

            @Override
            public Optional<DataRow> findFirst() {
                try (Stream<DataRow> s = stream()) {
                    return s.findFirst();
                }
            }
        };
    }

    @Override
    public SimpleDMLExecutor table(@NotNull String name) {
        if (Objects.nonNull(xqlFileManager)) {
            name = SqlUtil.formatSql(name, xqlFileManager.getConstants());
            SqlUtil.assertInvalidIdentifier(name);
        }
        @NotNull final String finalName = name;
        return new SimpleDMLExecutor() {
            @Override
            public Conditional where(@NotNull String condition) {
                return new Conditional() {
                    String genUpdateStatement(Map<String, ?> args) {
                        Set<String> whereArgs = sqlGenerator.generatePreparedSql(condition, args)
                                .getArgNameIndexMapping()
                                .keySet();
                        Set<String> setsFields = new HashSet<>();
                        for (Map.Entry<String, ?> arg : args.entrySet()) {
                            if (!whereArgs.contains(arg.getKey())) {
                                setsFields.add(arg.getKey());
                            }
                        }
                        if (setsFields.isEmpty()) {
                            throw new IllegalArgumentException("Set data is empty.");
                        }
                        return sqlGenerator.generateNamedParamUpdate(finalName, setsFields) + " where " + condition;
                    }

                    @Override
                    public int update(@NotNull Map<String, ?> args) {
                        return executeUpdate(genUpdateStatement(args), args);
                    }

                    @Override
                    public int update(@NotNull Iterable<? extends Map<String, ?>> args) {
                        return update(args, Function.identity());
                    }

                    @Override
                    public <T> int update(@NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
                        Map<String, ?> first = argMapper.apply(args.iterator().next());
                        return executeBatchUpdate(genUpdateStatement(first), args, argMapper, batchSize);
                    }

                    @Override
                    public int delete(@NotNull Map<String, ?> args) {
                        return executeUpdate("delete from " + finalName + " where " + condition, args);
                    }

                    @Override
                    public int delete(@NotNull Iterable<? extends Map<String, ?>> args) {
                        return delete(args, Function.identity());
                    }

                    @Override
                    public <T> int delete(@NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
                        return executeBatchUpdate("delete from " + finalName + " where " + condition, args, argMapper, batchSize);
                    }
                };
            }

            @Override
            public int insert(@NotNull Map<String, ?> data) {
                String insert = sqlGenerator.generateNamedParamInsert(finalName, data.keySet());
                return executeUpdate(insert, data);
            }

            @Override
            public int insert(@NotNull Iterable<? extends Map<String, ?>> data) {
                return insert(data, Function.identity());
            }

            @Override
            public <T> int insert(@NotNull Iterable<T> data, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
                Map<String, ?> first = argMapper.apply(data.iterator().next());
                String insert = sqlGenerator.generateNamedParamInsert(finalName, first.keySet());
                return executeBatchUpdate(insert, data, argMapper, batchSize);
            }

            @Override
            public List<String> fields() {
                return BakiDao.this.using(c -> {
                    try {
                        Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery("select * from " + finalName + " where 1 = 2");
                        List<String> fields = Arrays.asList(JdbcUtil.createNames(rs, ""));
                        JdbcUtil.closeResultSet(rs);
                        JdbcUtil.closeStatement(s);
                        return fields;
                    } catch (SQLException e) {
                        throw new SqlRuntimeException("Query '" + finalName + "' fields error", e);
                    }
                });
            }
        };
    }

    @Override
    public int insert(@NotNull String sql, @NotNull Map<String, ?> data) {
        return executeUpdate(sql, data);
    }

    @Override
    public int insert(@NotNull String sql, @NotNull Iterable<? extends Map<String, ?>> data) {
        return executeBatchUpdate(sql, data, Function.identity(), batchSize);
    }

    @Override
    public <T> int insert(@NotNull String sql, @NotNull Iterable<T> data, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
        return executeBatchUpdate(sql, data, argMapper, batchSize);
    }

    @Override
    public int update(@NotNull String sql, Map<String, ?> args) {
        return executeUpdate(sql, args);
    }

    @Override
    public int update(@NotNull String sql, @NotNull Iterable<? extends Map<String, ?>> args) {
        return executeBatchUpdate(sql, args, Function.identity(), batchSize);
    }

    @Override
    public <T> int update(@NotNull String sql, @NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
        return executeBatchUpdate(sql, args, argMapper, batchSize);
    }

    @Override
    public int delete(@NotNull String sql, Map<String, ?> args) {
        return executeUpdate(sql, args);
    }

    @Override
    public int delete(@NotNull String sql, @NotNull Iterable<? extends Map<String, ?>> args) {
        return executeBatchUpdate(sql, args, Function.identity(), batchSize);
    }

    @Override
    public <T> int delete(@NotNull String sql, @NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
        return executeBatchUpdate(sql, args, argMapper, batchSize);
    }

    @Override
    public @NotNull DataRow call(@NotNull String procedure, Map<String, Param> params) {
        return executeCallStatement(procedure, params);
    }

    @Override
    public @NotNull DataRow execute(@NotNull String sql, Map<String, ?> args) {
        return executeAny(sql, args);
    }

    @Override
    public int execute(@NotNull String sql, @NotNull Iterable<? extends Map<String, ?>> args) {
        return executeBatchUpdate(sql, args, Function.identity(), batchSize);
    }

    @Override
    public <T> int execute(@NotNull String sql, @NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
        return executeBatchUpdate(sql, args, argMapper, batchSize);
    }

    @Override
    public int execute(@NotNull Iterable<String> sqlList) {
        return executeBatch(sqlList, batchSize);
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
    public @NotNull DatabaseMetaData metaData() {
        return this.metaData;
    }

    @Override
    public @NotNull String databaseId() {
        return this.databaseId;
    }

    /**
     * Simple page helper implementation.
     */
    final class SimplePageable extends IPageable {
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
            // fetch the page helper.
            PageHelper pageHelper = null;
            if (pageHelperProvider != null) {
                pageHelper = pageHelperProvider.customPageHelper(metaData, databaseId, namedParamPrefix);
            }
            if (pageHelper == null) {
                pageHelper = builtinPager();
            }
            boolean isSqlRef = recordQuery.startsWith("&");
            if (count == null) {
                String finalCountQuery = countQuery;
                if (finalCountQuery == null) {
                    if (isSqlRef) {
                        finalCountQuery = recordQuery + "^" + SQL_REF_MODIFIER_COUNT;
                        args.put(INTERNAL_PAGE_HELPER_ARG_KEY, pageHelper);
                    } else {
                        finalCountQuery = pageHelper.countSql(recordQuery);
                    }
                }
                try (Stream<DataRow> s = executeQueryStream(finalCountQuery, args)) {
                    count = s.findFirst()
                            .map(d -> d.getInt(0))
                            .orElse(0);
                }
            }

            if (count == 0) {
                return PagedResource.empty(page, size);
            }

            pageHelper.init(page, size, count);

            Args<Integer> pagedArgs = pageHelper.pagedArgs();
            args.putAll(rewriteArgsFunc == null ? pagedArgs : rewriteArgsFunc.apply(pagedArgs));

            String pageQuery;
            if (disablePageSql) {
                pageQuery = recordQuery;
            } else {
                if (isSqlRef) {
                    pageQuery = recordQuery + "^" + SQL_REF_MODIFIER_PAGE;
                    args.put(INTERNAL_PAGE_HELPER_ARG_KEY, pageHelper);
                } else {
                    pageQuery = pageHelper.pagedSql(namedParamPrefix, recordQuery);
                }
            }
            try (Stream<DataRow> s = executeQueryStream(pageQuery, args)) {
                List<T> list = s.peek(d -> d.remove(PageHelper.ROW_NUM_KEY))
                        .map(mapper)
                        .collect(Collectors.toList());
                return PagedResource.of(pageHelper, list);
            }
        }
    }

    /**
     * Built-in default page helper.
     *
     * @return PageHelper instance
     * @throws UnsupportedOperationException there is no default implementation of your database
     * @throws ConnectionStatusException     connection status exception
     */
    protected PageHelper builtinPager() {
        if (Objects.nonNull(globalPageHelperProvider)) {
            PageHelper pageHelper = globalPageHelperProvider.customPageHelper(metaData, databaseId, namedParamPrefix);
            if (Objects.nonNull(pageHelper)) {
                return pageHelper;
            }
        }
        switch (databaseId) {
            case "oracle":
            case "dm dbms":
                return new OraclePageHelper();
            case "postgresql":
            case "sqlite":
            case "kingbasees":
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
            case "microsoft sql server":
                return new SqlServer2012PageHelper();
            default:
                throw new UnsupportedOperationException("Pager of \"" + databaseId + "\" default not implement currently, see method 'setGlobalPageHelperProvider'.");
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
     */
    @Override
    protected SqlGenerator.PreparedSqlMetaData prepareSql(@NotNull String sql, Map<String, ?> args) {
        Map<String, Object> myArgs = new HashMap<>();
        if (Objects.nonNull(args)) {
            myArgs.putAll(args);
        }
        String mySql = sql.trim();
        if (mySql.startsWith("&")) {
            log.debug("SQL: {}", mySql);
            String sqlRef = mySql.substring(1);
            Pair<String, Map<String, Object>> result = xqlFileManager.get(sqlRef, myArgs);
            mySql = result.getItem1();
            myArgs.putAll(result.getItem2());

            String modifier = XQLFileManager.extractModifier(sqlRef);
            if (Objects.nonNull(modifier)) {
                switch (modifier) {
                    case SQL_REF_MODIFIER_PAGE:
                    case SQL_REF_MODIFIER_COUNT:
                        Object obj = myArgs.get(INTERNAL_PAGE_HELPER_ARG_KEY);
                        if (obj instanceof PageHelper) {
                            PageHelper pageHelper = (PageHelper) obj;
                            if (modifier.equals(SQL_REF_MODIFIER_COUNT)) {
                                mySql = pageHelper.countSql(mySql);
                            } else {
                                mySql = pageHelper.pagedSql(namedParamPrefix, mySql);
                            }
                        }
                        break;
                }
            }
        }
        if (Objects.nonNull(sqlInterceptor)) {
            mySql = sqlInterceptor.preHandle(sql.trim(), mySql, myArgs, metaData);
        }
        if (mySql.contains("${")) {
            mySql = SqlUtil.formatSql(mySql, myArgs);
            if (Objects.nonNull(xqlFileManager)) {
                mySql = SqlUtil.formatSql(mySql, xqlFileManager.getConstants());
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("SQL: {}", SqlHighlighter.highlightIfAnsiCapable(mySql));
            StringJoiner sj = new StringJoiner(", ", "{", "}");
            myArgs.forEach((k, v) -> {
                if (v == null) {
                    sj.add(k + "=null");
                } else {
                    sj.add(k + "=" + v + "(" + v.getClass().getSimpleName() + ")");
                }
            });
            log.debug("Args: {}", sj);
        }
        return sqlGenerator.generatePreparedSql(mySql, myArgs);
    }

    @Override
    protected @NotNull DataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected @NotNull Connection getConnection() {
        try {
            return DataSourceUtil.getConnection(dataSource);
        } catch (SQLException e) {
            throw new ConnectionStatusException("Fetch connection failed.", e);
        }
    }

    @Override
    protected void releaseConnection(Connection connection, DataSource dataSource) {
        DataSourceUtil.releaseConnection(connection, dataSource);
    }

    @Override
    protected void doHandleStatementValue(@NotNull PreparedStatement ps,
                                          @Range(from = 1, to = Integer.MAX_VALUE) int index,
                                          @Nullable Object value) throws SQLException {
        statementValueHandler.handle(ps, index, value, metaData);
    }

    @Override
    protected int queryTimeout(String sql, Map<String, ?> args) {
        return queryTimeoutHandler.handle(sql, args);
    }

    public SqlGenerator getSqlGenerator() {
        return sqlGenerator;
    }

    public void setGlobalPageHelperProvider(PageHelperProvider globalPageHelperProvider) {
        this.globalPageHelperProvider = globalPageHelperProvider;
    }

    public PageHelperProvider getGlobalPageHelperProvider() {
        return globalPageHelperProvider;
    }

    public void setSqlInterceptor(SqlInterceptor sqlInterceptor) {
        this.sqlInterceptor = sqlInterceptor;
    }

    public SqlInterceptor getSqlInterceptor() {
        return sqlInterceptor;
    }

    public void setStatementValueHandler(StatementValueHandler statementValueHandler) {
        if (Objects.nonNull(statementValueHandler))
            this.statementValueHandler = statementValueHandler;
    }

    public StatementValueHandler getStatementValueHandler() {
        return statementValueHandler;
    }

    public void setXqlFileManager(XQLFileManager xqlFileManager) {
        if (Objects.nonNull(xqlFileManager)) {
            this.xqlFileManager = xqlFileManager;
            this.xqlFileManager.setDatabaseId(databaseId);
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

    public void setBatchSize(@Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        this.batchSize = batchSize;
    }

    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }

    public void setNamedParamPrefix(char namedParamPrefix) {
        this.namedParamPrefix = namedParamPrefix;
        this.sqlGenerator = new SqlGenerator(this.namedParamPrefix);
    }

    public String getPageKey() {
        return pageKey;
    }

    public void setPageKey(@NotNull String pageKey) {
        this.pageKey = pageKey;
    }

    public String getSizeKey() {
        return sizeKey;
    }

    public void setSizeKey(@NotNull String sizeKey) {
        this.sizeKey = sizeKey;
    }

    public void setQueryTimeoutHandler(QueryTimeoutHandler queryTimeoutHandler) {
        if (Objects.nonNull(queryTimeoutHandler)) {
            this.queryTimeoutHandler = queryTimeoutHandler;
        }
    }

    public QueryTimeoutHandler getQueryTimeoutHandler() {
        return queryTimeoutHandler;
    }

    public void setSqlInvokeHandler(SqlInvokeHandler sqlInvokeHandler) {
        if (Objects.nonNull(sqlInvokeHandler)) {
            this.sqlInvokeHandler = sqlInvokeHandler;
        }
    }

    public SqlInvokeHandler getSqlInvokeHandler() {
        return sqlInvokeHandler;
    }

    public QueryCacheManager getQueryCacheManager() {
        return queryCacheManager;
    }

    public void setQueryCacheManager(QueryCacheManager queryCacheManager) {
        this.queryCacheManager = queryCacheManager;
    }

    public ExecutionWatcher getExecutionWatcher() {
        return executionWatcher;
    }

    public void setExecutionWatcher(ExecutionWatcher executionWatcher) {
        this.executionWatcher = executionWatcher;
    }

    public EntityFieldMapper getEntityFieldMapper() {
        return entityFieldMapper;
    }

    public void setEntityFieldMapper(EntityFieldMapper entityFieldMapper) {
        this.entityFieldMapper = entityFieldMapper;
    }

    public EntityValueMapper getEntityValueMapper() {
        return entityValueMapper;
    }

    public void setEntityValueMapper(EntityValueMapper entityValueMapper) {
        this.entityValueMapper = entityValueMapper;
    }
}
