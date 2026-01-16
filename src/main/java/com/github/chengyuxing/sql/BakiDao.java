package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.AroundExecutor;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.MethodReference;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.common.util.ReflectUtils;
import com.github.chengyuxing.sql.datasource.DataSourceUtils;
import com.github.chengyuxing.sql.dsl.Delete;
import com.github.chengyuxing.sql.dsl.Insert;
import com.github.chengyuxing.sql.dsl.Query;
import com.github.chengyuxing.sql.dsl.Update;
import com.github.chengyuxing.sql.dsl.clause.OrderBy;
import com.github.chengyuxing.sql.dsl.clause.Where;
import com.github.chengyuxing.sql.dsl.clause.condition.Criteria;
import com.github.chengyuxing.sql.dsl.types.OrderByType;
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
    private static final String INTERNAL_PAGE_HELPER_ARG_KEY = "_$rabbit.sql.pageHelper";
    private static final String SQL_REF_MODIFIER_COUNT = "count";
    private static final String SQL_REF_MODIFIER_PAGE = "page";
    private final DataSource dataSource;
    private DatabaseMetaData metaData;
    private String databaseId;
    private SqlGenerator sqlGenerator;
    private EntityManager entityManager;
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
        this.entityManager = new EntityManager(namedParamPrefix);
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
        this.statementValueHandler = (ps, index, value, metaData) -> JdbcUtils.setStatementValue(ps, index, value);
        this.queryTimeoutHandler = (sql, args) -> 0;
        this.sqlInvokeHandler = type -> null;
        this.using(c -> {
            try {
                this.metaData = c.getMetaData();
                this.databaseId = this.metaData.getDatabaseProductName().toLowerCase();
                return 0;
            } catch (SQLException e) {
                throw new IllegalStateException("Initialize metadata error.", e);
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
        return XQLMapperUtils.getProxyInstance(mapperInterface, new XQLInvocationHandler() {
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
                    if (queryCacheManager == null || !queryCacheManager.isAvailable(sql, args)) {
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
    public @NotNull QueryExecutor query(@NotNull String sql) {
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
                    return s.map(d -> d.toEntity(entityClass,
                            field -> getEntityMetaProvider().columnMeta(field).getName(),
                            getEntityMetaProvider()::columnValue
                    )).collect(Collectors.toList());
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
                return findFirst().map(d -> d.toEntity(entityClass,
                        field -> getEntityMetaProvider().columnMeta(field).getName(),
                        getEntityMetaProvider()::columnValue
                )).orElse(null);
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
    public @NotNull SimpleDMLExecutor table(@NotNull String name) {
        if (xqlFileManager != null) {
            name = SqlUtils.formatSql(name, xqlFileManager.getConstants());
            SqlUtils.assertInvalidIdentifier(name);
        }
        final String finalName = name;
        return new SimpleDMLExecutor() {
            boolean enableBatch = false;

            @Override
            public Conditional where(@NotNull String condition) {
                return new Conditional() {
                    Set<String> collectUpdateSetColumns(Map<String, ?> args) {
                        Set<String> whereArgs = sqlGenerator.generatePreparedSql(condition, args)
                                .getArgNameIndexMapping()
                                .keySet();
                        Set<String> setsFields = new HashSet<>();
                        for (Map.Entry<String, ?> arg : args.entrySet()) {
                            if (!whereArgs.contains(arg.getKey())) {
                                setsFields.add(arg.getKey());
                            }
                        }
                        return setsFields;
                    }

                    @Override
                    public int update(@NotNull Map<String, ?> args) {
                        Set<String> columns = collectUpdateSetColumns(args);
                        if (columns.isEmpty()) {
                            return 0;
                        }
                        String update = sqlGenerator.generateNamedParamUpdateBy(finalName, columns) + condition;
                        return executeUpdate(update, args);
                    }

                    @Override
                    public int update(@NotNull Iterable<? extends Map<String, ?>> args) {
                        return update(args, Function.identity());
                    }

                    @Override
                    public <T> int update(@NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
                        if (enableBatch) {
                            Map<String, ?> first = argMapper.apply(args.iterator().next());
                            Set<String> columns = collectUpdateSetColumns(first);
                            if (columns.isEmpty()) {
                                return 0;
                            }
                            String update = sqlGenerator.generateNamedParamUpdateBy(finalName, columns) + condition;
                            return executeBatchUpdate(update, args, argMapper, batchSize);
                        }
                        int n = 0;
                        for (T arg : args) {
                            n += update(argMapper.apply(arg));
                        }
                        return n;
                    }

                    @Override
                    public int delete(@NotNull Map<String, ?> args) {
                        String delete = sqlGenerator.generateDeleteBy(finalName) + condition;
                        return executeUpdate(delete, args);
                    }

                    @Override
                    public int delete(@NotNull Iterable<? extends Map<String, ?>> args) {
                        return delete(args, Function.identity());
                    }

                    @Override
                    public <T> int delete(@NotNull Iterable<T> args, @NotNull Function<T, ? extends Map<String, ?>> argMapper) {
                        String delete = sqlGenerator.generateDeleteBy(finalName) + condition;
                        return executeBatchUpdate(delete, args, argMapper, batchSize);
                    }
                };
            }

            @Override
            public SimpleDMLExecutor enableBatch() {
                enableBatch = true;
                return this;
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
                if (enableBatch) {
                    Map<String, ?> first = argMapper.apply(data.iterator().next());
                    String insert = sqlGenerator.generateNamedParamInsert(finalName, first.keySet());
                    return executeBatchUpdate(insert, data, argMapper, batchSize);
                }
                int n = 0;
                for (T arg : data) {
                    n += insert(argMapper.apply(arg));
                }
                return n;
            }

            @Override
            public List<String> fields() {
                return BakiDao.this.using(c -> {
                    //noinspection SqlConstantExpression
                    String query = "select * from " + finalName + " where 1 = 2";
                    try {
                        Statement s = c.createStatement();
                        //noinspection SqlSourceToSinkFlow
                        ResultSet rs = s.executeQuery(query);
                        List<String> fields = Arrays.asList(JdbcUtils.createNames(rs, ""));
                        JdbcUtils.closeResultSet(rs);
                        JdbcUtils.closeStatement(s);
                        return fields;
                    } catch (SQLException e) {
                        throw wrappedDataAccessException(query, e);
                    }
                });
            }
        };
    }

    @Override
    public <T> EntityExecutor<T> entity(@NotNull Class<T> clazz) {
        return new EntityExecutor<T>() {
            final EntityManager.EntityMeta entityMeta = entityManager.getEntityMeta(clazz);

            String parseMethodRefColumn(MethodReference<T> methodRef) {
                String fieldName = ReflectUtils.getFieldName(methodRef);
                try {
                    Field field = clazz.getDeclaredField(fieldName);
                    return getEntityMetaProvider().columnMeta(field).getName();
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }

            final class InternalWhere extends Where<T> {
                InternalWhere() {
                }

                InternalWhere(Where<T> where) {
                    super(where);
                }

                InternalWhere(List<Criteria> criteria) {
                    this.criteria.addAll(criteria);
                }

                @Override
                protected Where<T> newInstance() {
                    return new InternalWhere();
                }

                @Override
                protected char namedParamPrefix() {
                    return namedParamPrefix;
                }

                @Override
                protected @NotNull String getColumnName(@NotNull MethodReference<T> fieldReference) {
                    return parseMethodRefColumn(fieldReference);
                }

                private List<Criteria> getCriteria() {
                    return criteria;
                }

                private boolean isEmpty() {
                    return criteria.isEmpty();
                }

                private Pair<String, Map<String, Object>> buildWhere() {
                    return build();
                }
            }

            final class InternalOrderBy extends OrderBy<T> {
                InternalOrderBy() {
                }

                InternalOrderBy(OrderBy<T> orderBy) {
                    super(orderBy);
                }

                InternalOrderBy(Set<Pair<String, OrderByType>> orders) {
                    this.orders.addAll(orders);
                }

                @Override
                protected @NotNull String getColumnName(@NotNull MethodReference<T> fieldReference) {
                    return parseMethodRefColumn(fieldReference);
                }

                private Set<Pair<String, OrderByType>> getOrders() {
                    return orders;
                }

                private String buildOrderBy() {
                    return build();
                }
            }

            @Override
            public <SELF extends Query<T, SELF>> Query<T, SELF> query() {
                return query(null);
            }

            @Override
            public <SELF extends Query<T, SELF>> Query<T, SELF> query(@Nullable Object queryId) {
                return new Query<T, SELF>() {
                    final List<Criteria> whereCriteria = new ArrayList<>();
                    final Set<Pair<String, OrderByType>> orderByColumns = new LinkedHashSet<>();
                    final Set<String> selectColumns = new LinkedHashSet<>();

                    /**
                     * Create query sql object.
                     * @return [record query, count query, args]
                     */
                    @NotNull Triple<String, String, Map<String, Object>> createQuery() {
                        final InternalWhere where = new InternalWhere(whereCriteria);
                        final InternalOrderBy orderBy = new InternalOrderBy(orderByColumns);
                        // select a, b, c from table
                        String recordSelect = selectColumns.isEmpty() ? entityMeta.getSelect() : entityMeta.getSelect(selectColumns);
                        // select count(*) from table
                        String countSelect = entityMeta.getCountSelect();
                        // where
                        Map<String, Object> args = new HashMap<>();
                        args.put(IDENTIFIER, queryId);
                        if (!where.isEmpty()) {
                            Pair<String, Map<String, Object>> w = where.buildWhere();
                            recordSelect += "\nwhere " + w.getItem1();
                            countSelect += "\nwhere " + w.getItem1();
                            args.putAll(w.getItem2());
                        }
                        // order by
                        recordSelect += orderBy.buildOrderBy();
                        return Triple.of(recordSelect, countSelect, args);
                    }

                    @SafeVarargs
                    @Override
                    public final SELF select(MethodReference<T> column, MethodReference<T>... more) {
                        selectColumns.add(parseMethodRefColumn(column));
                        for (MethodReference<T> m : more) {
                            selectColumns.add(parseMethodRefColumn(m));
                        }
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public SELF where(@NotNull Function<Where<T>, Where<T>> where) {
                        InternalWhere gotten = new InternalWhere(where.apply(new InternalWhere()));
                        whereCriteria.addAll(gotten.getCriteria());
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public SELF orderBy(@NotNull Function<OrderBy<T>, OrderBy<T>> orderBy) {
                        InternalOrderBy gotten = new InternalOrderBy(orderBy.apply(new InternalOrderBy()));
                        orderByColumns.addAll(gotten.getOrders());
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public @NotNull Stream<T> stream() {
                        Triple<String, String, Map<String, Object>> query = createQuery();
                        return executeQueryStream(query.getItem1(), query.getItem3())
                                .map(d -> d.toEntity(clazz,
                                        f -> getEntityMetaProvider().columnMeta(f).getName(),
                                        getEntityMetaProvider()::columnValue
                                ));
                    }

                    @Override
                    public @NotNull List<T> list() {
                        try (Stream<T> s = stream()) {
                            return s.collect(Collectors.toList());
                        }
                    }

                    @Override
                    public @NotNull <R> List<R> list(@NotNull Function<T, R> mapper) {
                        try (Stream<T> s = stream()) {
                            return s.map(mapper).collect(Collectors.toList());
                        }
                    }

                    @Override
                    public @NotNull List<T> top(@Range(from = 1, to = Integer.MAX_VALUE) int n) {
                        Triple<String, String, Map<String, Object>> query = createQuery();
                        return new SimplePageable(query.getItem1(), 1, n)
                                .count(n)
                                .args(query.getItem3())
                                .collect(d -> d.toEntity(clazz,
                                        f -> getEntityMetaProvider().columnMeta(f).getName(),
                                        getEntityMetaProvider()::columnValue
                                ))
                                .getData();
                    }

                    @Override
                    public @NotNull Optional<T> findFirst() {
                        List<T> top1 = this.top(1);
                        if (top1.isEmpty()) {
                            return Optional.empty();
                        }
                        return Optional.of(top1.get(0));
                    }

                    @Override
                    public @NotNull PagedResource<T> pageable(@Range(from = 1, to = Integer.MAX_VALUE) int page, @Range(from = 1, to = Integer.MAX_VALUE) int size, @Nullable PageHelperProvider pageHelperProvider) {
                        Triple<String, String, Map<String, Object>> query = createQuery();
                        return new SimplePageable(query.getItem1(), page, size)
                                .count(query.getItem2())
                                .args(query.getItem3())
                                .pageHelper(pageHelperProvider)
                                .collect(d -> d.toEntity(clazz,
                                        f -> getEntityMetaProvider().columnMeta(f).getName(),
                                        getEntityMetaProvider()::columnValue
                                ));
                    }

                    @Override
                    public @NotNull PagedResource<T> pageable(@Range(from = 1, to = Integer.MAX_VALUE) int page, @Range(from = 1, to = Integer.MAX_VALUE) int size) {
                        return pageable(page, size, null);
                    }

                    @Override
                    public @Range(from = 0, to = Long.MAX_VALUE) long count() {
                        Triple<String, String, Map<String, Object>> query = createQuery();
                        try (Stream<DataRow> s = executeQueryStream(query.getItem2(), query.getItem3())) {
                            return s.findFirst()
                                    .map(d -> d.getLong(0))
                                    .orElse(0L);
                        }
                    }
                };
            }

            @Override
            public Insert<T> insert() {
                return new Insert<T>() {
                    final Map<String, EntityManager.ColumnMeta> columnMetas = entityMeta.getColumns();
                    final Map<String, EntityManager.ColumnMeta> insertColumns = entityMeta.getInsertColumns();
                    boolean withNullValues = false;

                    @Override
                    public Insert<T> withNullValues() {
                        withNullValues = true;
                        return this;
                    }

                    @Override
                    public int save(T entity) {
                        Args<Object> args = Args.ofEntity(entity, field -> getEntityMetaProvider().columnMeta(field).getName());
                        String insert;
                        if (withNullValues) {
                            if (args.get(entityMeta.getPrimaryKey()) == null) {
                                throw new IllegalArgumentException("Primary key must not be null");
                            }
                            insert = entityMeta.getInsert();
                        } else {
                            Map<String, EntityManager.ColumnMeta> columns = new HashMap<>(insertColumns);
                            columns.entrySet().removeIf(e -> args.get(e.getKey()) == null);
                            insert = entityMeta.getInsert(columns);
                        }
                        return executeUpdate(insert, args);
                    }

                    @Override
                    public int save(Iterable<T> entities) {
                        if (withNullValues) {
                            return executeBatchUpdate(entityMeta.getInsert(),
                                    entities,
                                    e -> {
                                        Args<Object> args = Args.ofEntity(e, field -> getEntityMetaProvider().columnMeta(field).getName());
                                        if (args.get(entityMeta.getPrimaryKey()) == null) {
                                            throw new IllegalArgumentException("Primary key must not be null");
                                        }
                                        return args;
                                    },
                                    batchSize);
                        }
                        int n = 0;
                        for (T entity : entities) {
                            n += save(entity);
                        }
                        return n;
                    }

                    @Override
                    public InsertSetter<T> set(MethodReference<T> column, Object value) {
                        return new InsertSetter<T>() {
                            final Map<String, Object> values = new HashMap<>();
                            final Map<String, EntityManager.ColumnMeta> columns = new HashMap<>();

                            void addColumn(MethodReference<T> column, Object value) {
                                String columnName = parseMethodRefColumn(column);
                                EntityManager.ColumnMeta columnMeta = columnMetas.get(columnName);
                                if (columnMeta == null) {
                                    throw new IllegalArgumentException("Cannot find column: " + columnName);
                                }
                                if (!columnMeta.isInsertable()) {
                                    throw new IllegalArgumentException("Cannot insert non-insertable column: " + columnName);
                                }
                                values.put(columnName, value);
                                columns.put(columnName, columnMeta);
                            }

                            @Override
                            public InsertSetter<T> set(MethodReference<T> column, Object value) {
                                addColumn(column, value);
                                return this;
                            }

                            @Override
                            public int save() {
                                addColumn(column, value);
                                String pk = entityMeta.getPrimaryKey();
                                if (values.containsKey(pk) && values.get(pk) == null) {
                                    throw new IllegalArgumentException("Cannot insert null primary key: " + entityMeta.getPrimaryKey());
                                }
                                String insert = entityMeta.getInsert(columns);
                                return executeUpdate(insert, values);
                            }
                        };
                    }
                };
            }

            @Override
            public Update<T> update() {
                return new Update<T>() {
                    final Map<String, EntityManager.ColumnMeta> columnMetas = entityMeta.getColumns();
                    final Map<String, EntityManager.ColumnMeta> updateColumnMetas = entityMeta.getUpdateColumns();
                    boolean withNullValues = false;

                    @Override
                    public Update<T> withNullValues() {
                        withNullValues = true;
                        return this;
                    }

                    @Override
                    public int save(@NotNull T entity) {
                        Args<Object> args = Args.ofEntity(entity, field -> getEntityMetaProvider().columnMeta(field).getName());
                        if (args.get(entityMeta.getPrimaryKey()) == null) {
                            throw new IllegalArgumentException("Cannot update entity with null primary key");
                        }
                        String update;
                        if (withNullValues) {
                            update = entityMeta.getUpdateById();
                        } else {
                            Map<String, EntityManager.ColumnMeta> columns = new HashMap<>(updateColumnMetas);
                            columns.entrySet().removeIf(e -> args.get(e.getKey()) == null);
                            if (columns.isEmpty()) {
                                return 0;
                            }
                            update = entityMeta.getUpdateBy(columns) + entityMeta.getIdCondition();
                        }
                        return executeUpdate(update, args);
                    }

                    @Override
                    public int save(@NotNull Iterable<T> entities) {
                        if (withNullValues) {
                            return executeBatchUpdate(entityMeta.getUpdateById(),
                                    entities,
                                    e -> {
                                        Args<Object> args = Args.ofEntity(e, field -> getEntityMetaProvider().columnMeta(field).getName());
                                        if (args.get(entityMeta.getPrimaryKey()) == null) {
                                            throw new IllegalArgumentException("Cannot update entity with null primary key");
                                        }
                                        return args;
                                    },
                                    batchSize);
                        }
                        int n = 0;
                        for (T entity : entities) {
                            n += save(entity);
                        }
                        return n;
                    }

                    @Override
                    public UpdateSetter<T> where(Function<Where<T>, Where<T>> where) {
                        return new UpdateSetter<T>() {
                            final Map<String, Object> sets = new HashMap<>();
                            final Map<String, EntityManager.ColumnMeta> columns = new HashMap<>();

                            @Override
                            public UpdateSetter<T> set(@NotNull MethodReference<T> column, Object value) {
                                String columnName = parseMethodRefColumn(column);
                                EntityManager.ColumnMeta columnMeta = columnMetas.get(columnName);
                                if (columnMeta == null) {
                                    throw new IllegalArgumentException("Cannot find column: " + columnName);
                                }
                                if (columnMeta.isPrimaryKey()) {
                                    throw new IllegalArgumentException("Cannot update primary key column: " + columnName);
                                }
                                if (!columnMeta.isUpdatable()) {
                                    throw new IllegalArgumentException("Cannot update non-updatable column: " + columnName);
                                }
                                sets.put(columnName, value);
                                columns.put(columnName, columnMeta);
                                return this;
                            }

                            @Override
                            public int save() {
                                if (sets.isEmpty()) {
                                    return 0;
                                }
                                final InternalWhere gotten = new InternalWhere(where.apply(new InternalWhere()));
                                if (gotten.isEmpty()) {
                                    throw new IllegalArgumentException("Cannot update without where condition");
                                }
                                Pair<String, Map<String, Object>> w = gotten.buildWhere();
                                Map<String, Object> data = new HashMap<>(sets);
                                data.putAll(w.getItem2());
                                String update = entityMeta.getUpdateBy(columns) + w.getItem1();
                                return executeUpdate(update, data);
                            }
                        };
                    }
                };
            }

            @Override
            public Delete<T> delete() {
                return new Delete<T>() {
                    @Override
                    public int execute(@NotNull T entity) {
                        Args<Object> args = Args.ofEntity(entity, field -> getEntityMetaProvider().columnMeta(field).getName());
                        if (args.get(entityMeta.getPrimaryKey()) == null) {
                            throw new IllegalArgumentException("Cannot delete entity with null primary key");
                        }
                        return executeUpdate(entityMeta.getDeleteById(), args);
                    }

                    @Override
                    public int execute(@NotNull Iterable<T> entities) {
                        return executeBatchUpdate(entityMeta.getDeleteById(), entities, e -> {
                            Args<Object> args = Args.ofEntity(e, field -> getEntityMetaProvider().columnMeta(field).getName());
                            if (args.get(entityMeta.getPrimaryKey()) == null) {
                                throw new IllegalArgumentException("Cannot delete entity with null primary key");
                            }
                            return args;
                        }, batchSize);
                    }

                    @Override
                    public Conditional where(Function<Where<T>, Where<T>> where) {
                        return () -> {
                            final InternalWhere gotten = new InternalWhere(where.apply(new InternalWhere()));
                            if (gotten.isEmpty()) {
                                throw new IllegalArgumentException("Cannot delete without where condition");
                            }
                            Pair<String, Map<String, Object>> w = gotten.buildWhere();
                            String delete = entityMeta.getDeleteBy() + w.getItem1();
                            return executeUpdate(delete, w.getItem2());
                        };
                    }
                };
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
        } catch (Exception e) {
            throw wrappedDataAccessException(null, e);
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
     */
    protected PageHelper builtinPager() {
        if (globalPageHelperProvider != null) {
            PageHelper pageHelper = globalPageHelperProvider.customPageHelper(metaData, databaseId, namedParamPrefix);
            if (pageHelper != null) {
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
        if (args != null) {
            myArgs.putAll(args);
        }
        String mySql = sql.trim();
        if (mySql.startsWith("&")) {
            log.debug("SQL Name: {}", mySql);
            String sqlRef = mySql.substring(1);
            Pair<String, Map<String, Object>> result = xqlFileManager.get(sqlRef, myArgs);
            mySql = result.getItem1();
            myArgs.putAll(result.getItem2());

            String modifier = XQLFileManager.extractModifier(sqlRef);
            if (modifier != null) {
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
        if (sqlInterceptor != null) {
            mySql = sqlInterceptor.preHandle(sql.trim(), mySql, myArgs, metaData);
        }
        if (mySql.contains("${")) {
            mySql = SqlUtils.formatSql(mySql, myArgs);
            if (xqlFileManager != null) {
                mySql = SqlUtils.formatSql(mySql, xqlFileManager.getConstants());
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
            return DataSourceUtils.getConnection(dataSource);
        } catch (SQLException e) {
            throw new IllegalStateException("Fetch connection failed.", e);
        }
    }

    @Override
    protected void releaseConnection(Connection connection, DataSource dataSource) {
        DataSourceUtils.releaseConnection(connection, dataSource);
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

    public EntityManager getEntityManager() {
        return entityManager;
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
        if (statementValueHandler != null)
            this.statementValueHandler = statementValueHandler;
    }

    public StatementValueHandler getStatementValueHandler() {
        return statementValueHandler;
    }

    public void setXqlFileManager(XQLFileManager xqlFileManager) {
        if (xqlFileManager != null) {
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
        if (queryTimeoutHandler != null) {
            this.queryTimeoutHandler = queryTimeoutHandler;
        }
    }

    public QueryTimeoutHandler getQueryTimeoutHandler() {
        return queryTimeoutHandler;
    }

    public void setSqlInvokeHandler(SqlInvokeHandler sqlInvokeHandler) {
        if (sqlInvokeHandler != null) {
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

    public EntityManager.EntityMetaProvider getEntityMetaProvider() {
        return entityManager.getEntityMetaProvider();
    }

    public void setEntityMetaProvider(EntityManager.EntityMetaProvider entityMetaProvider) {
        if (entityMetaProvider != null) {
            this.entityManager.setEntityMetaProvider(entityMetaProvider);
        }
    }
}
