package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.AroundExecutor;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.sql.datasource.DataSourceUtil;
import com.github.chengyuxing.sql.dsl.*;
import com.github.chengyuxing.sql.dsl.clause.*;
import com.github.chengyuxing.sql.dsl.clause.condition.Criteria;
import com.github.chengyuxing.sql.dsl.types.FieldReference;
import com.github.chengyuxing.sql.dsl.types.OrderByType;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.*;
import com.github.chengyuxing.sql.plugins.*;
import com.github.chengyuxing.sql.support.*;
import com.github.chengyuxing.sql.support.executor.EntityExecutor;
import com.github.chengyuxing.sql.support.executor.GenericExecutor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.annotation.SqlStatementType;
import com.github.chengyuxing.sql.types.SqlStatement;
import com.github.chengyuxing.sql.utils.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final Map<SqlStatementType, SqlInvokeHandler> xqlMappingHandlers = new HashMap<>();
    private final Map<String, Object> queryCacheLocks = new ConcurrentHashMap<>();
    private final DataSource dataSource;
    private DatabaseMetaData metaData;
    private String databaseId;
    private SqlGenerator sqlGenerator;
    private EntityManager entityManager;
    private AroundExecutor<SqlStatement> sqlAroundExecutor;

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
    private SqlParseChecker sqlParseChecker;
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
     * Query cache manager.
     */
    private QueryCacheManager queryCacheManager;
    /**
     * Query dsl condition operator whitelist.
     */
    private Set<String> operatorWhiteList;

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
        this.sqlAroundExecutor = new AroundExecutor<>() {
            @Override
            protected void onStart(@NotNull SqlStatement identifier) {
                identifier.setState("startTime", System.currentTimeMillis());
            }

            @Override
            protected void onStop(@NotNull SqlStatement identifier, @Nullable Object result, @Nullable Throwable throwable) {
                // Watch execute sql.
                if (Objects.nonNull(sqlWatcher))
                    sqlWatcher.watch(identifier.getSql(), identifier.getArgs(), identifier.getState("startTime"), System.currentTimeMillis(), throwable);
            }
        };
        this.statementValueHandler = (ps, index, value, metaData) -> JdbcUtil.setStatementValue(ps, index, value);
        this.queryTimeoutHandler = (sql, args) -> 0;
        this.using(c -> {
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
    public DataRow execute(@NotNull String sql, Map<String, ?> args) {
        return this.sqlAroundExecutor.call(new SqlStatement(SqlStatementType.unset, sql, args),
                i -> super.execute(sql, args));
    }

    @Override
    public Stream<DataRow> executeQueryStream(@NotNull String sql, Map<String, Object> args) {
        return this.sqlAroundExecutor.call(new SqlStatement(SqlStatementType.query, sql, args),
                i -> {
                    if (Objects.isNull(queryCacheManager) || !queryCacheManager.isAvailable(sql, args)) {
                        return super.executeQueryStream(sql, args);
                    }
                    String uniqueKey = queryCacheManager.uniqueKey(sql, args);
                    Stream<DataRow> cache = queryCacheManager.get(uniqueKey);
                    if (Objects.nonNull(cache)) {
                        log.debug("Hits cache({}, {}), returns data from cache.", sql, args);
                        return cache;
                    }
                    Object lock = queryCacheLocks.computeIfAbsent(uniqueKey, k -> new Object());
                    synchronized (lock) {
                        cache = queryCacheManager.get(uniqueKey);
                        if (Objects.nonNull(cache)) {
                            log.debug("Hits cache({}, {}) after lock, returns data from cache.", sql, args);
                            return cache;
                        }
                        List<DataRow> prepareCache = new ArrayList<>();
                        Stream<DataRow> queryStream = super.executeQueryStream(sql, args)
                                .peek(prepareCache::add)
                                .onClose(() -> queryCacheManager.put(uniqueKey, prepareCache));
                        log.debug("Put query result({}, {}) to cache.", sql, args);
                        return queryStream;
                    }
                });
    }

    @Override
    public int executeUpdate(@NotNull String sql, Map<String, ?> args) {
        return this.sqlAroundExecutor.call(new SqlStatement(SqlStatementType.dml, sql, args),
                i -> super.executeUpdate(sql, args));
    }

    @Override
    public int executeBatchUpdate(@NotNull String sql, @NotNull Collection<? extends Map<String, ?>> args, @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        return this.sqlAroundExecutor.call(new SqlStatement(SqlStatementType.dml, sql, args),
                i -> super.executeBatchUpdate(sql, args, batchSize));
    }

    @Override
    public DataRow executeCallStatement(@NotNull String procedure, Map<String, Param> args) {
        return this.sqlAroundExecutor.call(new SqlStatement(SqlStatementType.procedure, procedure, args),
                i -> super.executeCallStatement(procedure, args));
    }

    @Override
    public int executeBatch(@NotNull List<String> sqls, @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        return this.sqlAroundExecutor.call(new SqlStatement(SqlStatementType.batch, String.join(";", sqls), null),
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
                    return s.map(d -> EntityUtil.mapToEntity(d, entityClass)).collect(Collectors.toList());
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
                    throw new IllegalArgumentException("page or size is null.");
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
                return findFirst().map(d -> EntityUtil.mapToEntity(d, entityClass)).orElse(null);
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
    public <T> EntityExecutor<T> entity(@NotNull Class<T> clazz) {
        return new EntityExecutor<T>() {
            final EntityManager.EntityMeta entityMeta = entityManager.getEntityMeta(clazz);

            @Override
            public <SELF extends Query<T, SELF>> Query<T, SELF> query() {
                return new Query<T, SELF>() {
                    private final Set<String> selectColumns = new LinkedHashSet<>();
                    private final List<Criteria> whereCriteria = new ArrayList<>();
                    private final Set<Pair<String, OrderByType>> orderByColumns = new LinkedHashSet<>();
                    private final Set<Pair<String, String>> groupByAggColumns = new LinkedHashSet<>();
                    private final Set<String> groupByColumns = new LinkedHashSet<>();
                    private final List<Criteria> havingCriteria = new ArrayList<>();
                    private int topN = 0;

                    /**
                     * Create query sql object.
                     * @param enableTopN do limit top number or not
                     * @return [record query, count query, args]
                     */
                    private Triple<String, String, Map<String, Object>> createQuery(boolean enableTopN) {
                        final InternalWhere<T> where = new InternalWhere<>(clazz, whereCriteria);
                        final InternalGroupBy<T> groupBy = new InternalGroupBy<>(clazz, groupByAggColumns, groupByColumns);
                        final InternalHaving<T> having = new InternalHaving<>(clazz, havingCriteria);
                        final InternalOrderBy<T> orderBy = new InternalOrderBy<>(clazz, orderByColumns, groupByAggColumns);

                        boolean hasGroupBy = !groupByColumns.isEmpty();

                        // select a, b, c from table
                        String recordSelect = selectColumns.isEmpty() ? entityMeta.getSelect() : entityMeta.getSelect(selectColumns);
                        // select count(*) from table
                        String countSelect = entityMeta.getCountSelect();
                        if (hasGroupBy) {
                            // select a, max(a), count(*) from table
                            recordSelect = entityMeta.getSelect(groupBy.getAllSelectColumns());
                            // select a from table
                            countSelect = entityMeta.getSelect(groupBy.getGroupColumns());
                        }

                        // where
                        Pair<String, Map<String, Object>> w = where.getWhereClause();
                        recordSelect += w.getItem1();
                        countSelect += w.getItem1();

                        // group by
                        recordSelect += groupBy.getGroupByClause();
                        countSelect += groupBy.getGroupByClause();

                        // having
                        Pair<String, Map<String, Object>> h = having.getHavingClause();
                        recordSelect += h.getItem1();
                        countSelect += h.getItem1();

                        // order by
                        recordSelect += orderBy.getOrderBy();

                        // select count(*) from (select id from table group by id) t
                        if (hasGroupBy) {
                            countSelect = sqlGenerator.generateCountQuery(countSelect);
                        }

                        Map<String, Object> allArgs = new HashMap<>(w.getItem2());
                        allArgs.putAll(h.getItem2());

                        if (enableTopN && topN > 0) {
                            PageHelper pageHelper = builtinPager();
                            pageHelper.init(1, topN, topN);
                            recordSelect = pageHelper.pagedSql(namedParamPrefix, recordSelect);
                            allArgs.putAll(pageHelper.pagedArgs());
                        }

                        return Triple.of(recordSelect, countSelect, allArgs);
                    }

                    @Override
                    public SELF where(@NotNull Function<Where<T>, Where<T>> where) {
                        InternalWhere<T> gotten = (InternalWhere<T>) where.apply(new InternalWhere<>(clazz));
                        whereCriteria.addAll(gotten.getCriteria());
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public SELF groupBy(@NotNull Function<GroupBy<T>, GroupBy<T>> groupBy) {
                        InternalGroupBy<T> gotten = (InternalGroupBy<T>) groupBy.apply(new InternalGroupBy<>(clazz));
                        groupByAggColumns.addAll(gotten.getAggColumns());
                        groupByColumns.addAll(gotten.getGroupColumns());
                        havingCriteria.addAll(gotten.getHavingCriteria());
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public SELF orderBy(@NotNull Function<OrderBy<T>, OrderBy<T>> orderBy) {
                        InternalOrderBy<T> gotten = (InternalOrderBy<T>) orderBy.apply(new InternalOrderBy<>(clazz));
                        orderByColumns.addAll(gotten.getOrders());
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public SELF select(@NotNull List<FieldReference<T>> columns) {
                        selectColumns.clear();
                        for (FieldReference<T> column : columns) {
                            String columnName = EntityUtil.getFieldNameWithCache(column);
                            // excludes the field which annotated with @Transient
                            if (entityMeta.getColumns().containsKey(columnName)) {
                                selectColumns.add(columnName);
                            }
                        }
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public SELF deselect(@NotNull List<FieldReference<T>> columns) {
                        if (columns.isEmpty()) {
                            //noinspection unchecked
                            return (SELF) this;
                        }
                        selectColumns.clear();
                        Set<String> deselectColumns = columns.stream().map(EntityUtil::getFieldNameWithCache).collect(Collectors.toSet());
                        for (String column : entityMeta.getColumns().keySet()) {
                            if (!deselectColumns.contains(column)) {
                                selectColumns.add(column);
                            }
                        }
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public SELF top(@Range(from = 1, to = Integer.MAX_VALUE) int n) {
                        topN = n;
                        //noinspection unchecked
                        return (SELF) this;
                    }

                    @Override
                    public Stream<DataRow> toRowStream() {
                        Triple<String, String, Map<String, Object>> query = createQuery(true);
                        return executeQueryStream(query.getItem1(), query.getItem3());
                    }

                    @Override
                    public Stream<T> toStream() {
                        return toRowStream().map(d -> EntityUtil.mapToEntity(d, clazz));
                    }

                    @Override
                    public @NotNull List<T> toList() {
                        try (Stream<T> s = toStream()) {
                            return s.collect(Collectors.toList());
                        }
                    }

                    @Override
                    public <R> @NotNull List<R> toList(@NotNull Function<T, R> mapper) {
                        try (Stream<T> s = toStream()) {
                            return s.map(mapper).collect(Collectors.toList());
                        }
                    }

                    @Override
                    public @NotNull Optional<T> findFirst() {
                        try (Stream<T> s = toStream()) {
                            return s.findFirst();
                        }
                    }

                    @Override
                    public @Nullable T getFirst() {
                        return findFirst().orElse(null);
                    }

                    @Override
                    public @NotNull PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                                     @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                                                     @Nullable PageHelperProvider pageHelperProvider) {
                        Triple<String, String, Map<String, Object>> query = createQuery(false);
                        return new SimplePageable(query.getItem1(), page, size)
                                .count(query.getItem2())
                                .args(query.getItem3())
                                .pageHelper(pageHelperProvider)
                                .collect(d -> EntityUtil.mapToEntity(d, clazz));
                    }

                    @Override
                    public @NotNull PagedResource<T> toPagedResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                                     @Range(from = 1, to = Integer.MAX_VALUE) int size) {
                        return toPagedResource(page, size, null);
                    }

                    @Override
                    public @NotNull PagedResource<DataRow> toPagedRowResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                                              @Range(from = 1, to = Integer.MAX_VALUE) int size,
                                                                              @Nullable PageHelperProvider pageHelperProvider) {
                        Triple<String, String, Map<String, Object>> query = createQuery(false);
                        return new SimplePageable(query.getItem1(), page, size)
                                .count(query.getItem2())
                                .args(query.getItem3())
                                .pageHelper(pageHelperProvider)
                                .collect();
                    }

                    @Override
                    public @NotNull PagedResource<DataRow> toPagedRowResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                                              @Range(from = 1, to = Integer.MAX_VALUE) int size) {
                        return toPagedRowResource(page, size, null);
                    }

                    @Override
                    public boolean exists() {
                        Pair<String, Map<String, Object>> where = new InternalWhere<>(clazz, whereCriteria).getWhereClause();
                        if (where.getItem1().isEmpty()) {
                            throw new IllegalStateException("where condition is required.");
                        }
                        String query = entityMeta.getExistsSelect(where.getItem1());
                        try (Stream<DataRow> s = executeQueryStream(query, where.getItem2())) {
                            return s.findFirst().isPresent();
                        }
                    }

                    @Override
                    public @Range(from = 0, to = Long.MAX_VALUE) long count() {
                        Triple<String, String, Map<String, Object>> query = createQuery(true);
                        try (Stream<DataRow> s = executeQueryStream(query.getItem2(), query.getItem3())) {
                            return s.findFirst()
                                    .map(d -> d.getLong(0))
                                    .orElse(0L);
                        }
                    }

                    @Override
                    public @NotNull Optional<DataRow> findFirstRow() {
                        try (Stream<DataRow> s = toRowStream()) {
                            return s.findFirst();
                        }
                    }

                    @Override
                    public @NotNull DataRow getFirstRow() {
                        return findFirstRow().orElse(DataRow.of());
                    }

                    @Override
                    public @NotNull List<DataRow> toRows() {
                        try (Stream<DataRow> s = toRowStream()) {
                            return s.collect(Collectors.toList());
                        }
                    }

                    @Override
                    public @NotNull List<Map<String, Object>> toMaps() {
                        try (Stream<DataRow> s = toRowStream()) {
                            return s.collect(Collectors.toList());
                        }
                    }

                    @Override
                    public @NotNull Pair<String, Map<String, Object>> getSql() {
                        Triple<String, String, Map<String, Object>> query = createQuery(true);
                        return Pair.of(query.getItem1(), Collections.unmodifiableMap(query.getItem3()));
                    }
                };
            }

            @Override
            public int insert(@NotNull T entity) {
                String insert = entityMeta.getInsert();
                Map<String, Object> data = Args.ofEntity(entity);
                return executeUpdate(insert, data);
            }

            @Override
            public int insert(@NotNull Collection<T> entities) {
                if (entities.isEmpty()) return 0;
                String insert = entityMeta.getInsert();
                List<Map<String, Object>> data = entities.stream()
                        .map(Args::ofEntity)
                        .collect(Collectors.toList());
                return executeBatchUpdate(insert, data, batchSize);
            }

            @Override
            public int update(@NotNull T entity, boolean ignoreNull) {
                Map<String, Object> data = Args.ofEntity(entity);
                String update = ignoreNull ? sqlGenerator.generateNamedParamUpdate(
                        entityMeta.getTableName(),
                        entityMeta.getUpdateColumns(),
                        data,
                        true
                ) + entityMeta.getWhereById() : entityMeta.getUpdateById();
                return executeUpdate(update, data);
            }

            @Override
            public int update(@NotNull T entity, boolean ignoreNull, Function<Where<T>, Where<T>> where) {
                InternalWhere<T> gotten = (InternalWhere<T>) where.apply(new InternalWhere<>(clazz));
                Triple<String, Map<String, Object>, Set<String>> whereClause = gotten.getWhereClause();
                Map<String, Object> data = Args.ofEntity(entity);
                data.putAll(whereClause.getItem2());

                Set<String> updateColumns = entityMeta.getUpdateColumns();
                updateColumns.removeAll(whereClause.getItem3());

                String update = sqlGenerator.generateNamedParamUpdate(
                        entityMeta.getTableName(),
                        updateColumns,
                        data,
                        ignoreNull
                ) + whereClause.getItem1();
                return executeUpdate(update, data);
            }

            @Override
            public int update(@NotNull Collection<T> entities, boolean ignoreNull) {
                if (entities.isEmpty()) return 0;
                List<Map<String, Object>> data = entities.stream()
                        .map(Args::ofEntity)
                        .collect(Collectors.toList());
                String update = ignoreNull ? sqlGenerator.generateNamedParamUpdate(
                        entityMeta.getTableName(),
                        entityMeta.getUpdateColumns(),
                        data.get(0),
                        true
                ) + entityMeta.getWhereById() : entityMeta.getUpdateById();
                return executeBatchUpdate(update, data, batchSize);
            }

            @Override
            public int update(@NotNull Collection<T> entities, boolean ignoreNull, Function<Where<T>, Where<T>> where) {
                if (entities.isEmpty()) return 0;
                InternalWhere<T> gotten = (InternalWhere<T>) where.apply(new InternalWhere<>(clazz));
                Triple<String, Map<String, Object>, Set<String>> whereClause = gotten.getWhereClause();
                List<Map<String, Object>> data = entities.stream()
                        .map(Args::ofEntity)
                        .peek(args -> args.putAll(whereClause.getItem2()))
                        .collect(Collectors.toList());

                Set<String> updateColumns = entityMeta.getUpdateColumns();
                updateColumns.removeAll(whereClause.getItem3());

                String update = sqlGenerator.generateNamedParamUpdate(
                        entityMeta.getTableName(),
                        updateColumns,
                        data.get(0),
                        ignoreNull
                ) + whereClause.getItem1();
                return executeBatchUpdate(update, data, batchSize);
            }

            @Override
            public int delete(@NotNull T entity) {
                String delete = entityMeta.getDeleteById();
                Map<String, Object> data = Args.ofEntity(entity);
                return executeUpdate(delete, data);
            }

            @Override
            public int delete(@NotNull T entity, Function<Where<T>, Where<T>> where) {
                InternalWhere<T> gotten = (InternalWhere<T>) where.apply(new InternalWhere<>(clazz));
                Pair<String, Map<String, Object>> whereClause = gotten.getWhereClause();
                String delete = entityMeta.getDeleteBy(whereClause.getItem1());
                Map<String, Object> data = Args.ofEntity(entity);
                data.putAll(whereClause.getItem2());
                return executeUpdate(delete, data);
            }

            @Override
            public int delete(@NotNull Collection<T> entities) {
                if (entities.isEmpty()) {
                    return 0;
                }
                String delete = entityMeta.getDeleteById();
                List<Map<String, Object>> data = entities.stream()
                        .map(Args::ofEntity)
                        .collect(Collectors.toList());
                return executeBatchUpdate(delete, data, batchSize);
            }

            @Override
            public int delete(@NotNull Collection<T> entities, Function<Where<T>, Where<T>> where) {
                if (entities.isEmpty()) {
                    return 0;
                }
                InternalWhere<T> gotten = (InternalWhere<T>) where.apply(new InternalWhere<>(clazz));
                Pair<String, Map<String, Object>> whereClause = gotten.getWhereClause();
                String delete = entityMeta.getDeleteBy(whereClause.getItem1());
                List<Map<String, Object>> data = entities.stream()
                        .map(Args::ofEntity)
                        .peek(args -> args.putAll(whereClause.getItem2()))
                        .collect(Collectors.toList());
                return executeBatchUpdate(delete, data, batchSize);
            }
        };
    }

    @Override
    public GenericExecutor of(@NotNull String sql) {
        return new GenericExecutor() {
            @Override
            public @NotNull DataRow execute() {
                return BakiDao.super.execute(sql, Collections.emptyMap());
            }

            @Override
            public @NotNull DataRow execute(Map<String, ?> args) {
                return BakiDao.super.execute(sql, args);
            }

            @Override
            public int executeBatch(@NotNull List<String> moreSql) {
                List<String> sqlList = new ArrayList<>(moreSql);
                sqlList.add(0, sql);
                return BakiDao.super.executeBatch(sqlList, batchSize);
            }

            @Override
            public int executeBatch(@NotNull Collection<? extends Map<String, ?>> data) {
                Map<String, ?> arg = data.isEmpty() ? new HashMap<>() : data.iterator().next();
                SqlGenerator.GeneratedSqlMetaData parsed = prepareSql(sql, arg);
                Collection<? extends Map<String, ?>> newData;
                if (parsed.getArgs().containsKey(XQLFileManager.DynamicSqlParser.FOR_VARS_KEY) &&
                        parsed.getPrepareSql().contains(XQLFileManager.DynamicSqlParser.VAR_PREFIX)) {
                    List<Map<String, Object>> list = new ArrayList<>();
                    for (Map<String, ?> item : data) {
                        list.add(prepareSql(sql, item).getArgs());
                    }
                    newData = list;
                } else {
                    newData = data;
                }
                return executeBatchUpdate(parsed.getPrepareSql(), newData, batchSize);
            }

            @Override
            public @NotNull DataRow call(Map<String, Param> params) {
                return executeCallStatement(sql, params);
            }
        };
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

    final class InternalWhere<T> extends Where<T> {
        private final Class<T> clazz;
        private final Set<String> columns = new HashSet<>();

        InternalWhere(Class<T> clazz) {
            super(clazz);
            this.clazz = clazz;
            this.columns.addAll(entityManager.getColumns(this.clazz).keySet());
        }

        InternalWhere(Class<T> clazz, List<Criteria> criteria) {
            super(clazz);
            this.clazz = clazz;
            this.columns.addAll(entityManager.getColumns(clazz).keySet());
            this.criteria.addAll(criteria);
        }

        @Override
        protected Where<T> newInstance() {
            return new InternalWhere<>(clazz);
        }

        @Override
        protected char namedParamPrefix() {
            return namedParamPrefix;
        }

        @Override
        protected Set<String> columnWhiteList() {
            return columns;
        }

        @Override
        protected Set<String> operatorWhiteList() {
            return operatorWhiteList;
        }

        private Triple<String, Map<String, Object>, Set<String>> getWhereClause() {
            return build();
        }

        private List<Criteria> getCriteria() {
            return criteria;
        }
    }

    final class InternalOrderBy<T> extends OrderBy<T> {
        private final Set<String> columns = new HashSet<>();

        InternalOrderBy(Class<T> clazz) {
            super(clazz);
            this.columns.addAll(entityManager.getColumns(clazz).keySet());
        }

        InternalOrderBy(Class<T> clazz, Set<Pair<String, OrderByType>> orders, Set<Pair<String, String>> groupByAggColumns) {
            super(clazz);
            this.orders.addAll(orders);
            this.columns.addAll(entityManager.getColumns(clazz).keySet());
            for (Pair<String, String> p : groupByAggColumns) {
                this.columns.add(p.getItem2());
            }
        }

        private Set<Pair<String, OrderByType>> getOrders() {
            return orders;
        }

        private String getOrderBy() {
            return build();
        }

        @Override
        protected Set<String> columnWhiteList() {
            return columns;
        }

        @Override
        protected Set<String> operatorWhiteList() {
            return operatorWhiteList;
        }
    }

    final class InternalGroupBy<T> extends GroupBy<T> {
        private final Set<String> columns = new HashSet<>();
        private final List<Criteria> havingCriteria = new ArrayList<>();

        InternalGroupBy(@NotNull Class<T> clazz) {
            super(clazz);
            this.columns.addAll(entityManager.getColumns(clazz).keySet());
        }

        InternalGroupBy(@NotNull Class<T> clazz, Set<Pair<String, String>> aggColumns, Set<String> groupColumns) {
            super(clazz);
            this.columns.addAll(entityManager.getColumns(clazz).keySet());
            this.groupColumns.addAll(groupColumns);
            this.aggColumns.addAll(aggColumns);
        }

        @Override
        public GroupBy<T> having(Function<Having<T>, Having<T>> having) {
            InternalHaving<T> gotten = (InternalHaving<T>) having.apply(new InternalHaving<>(clazz));
            havingCriteria.addAll(gotten.getCriteria());
            return this;
        }

        @Override
        protected Set<String> columnWhiteList() {
            return columns;
        }

        @Override
        protected Set<String> operatorWhiteList() {
            return operatorWhiteList;
        }

        private List<Criteria> getHavingCriteria() {
            return havingCriteria;
        }

        public String getGroupByClause() {
            return build();
        }

        private Set<String> getGroupColumns() {
            return groupColumns;
        }

        private Set<Pair<String, String>> getAggColumns() {
            return aggColumns;
        }

        private Set<String> getAllSelectColumns() {
            return getSelectColumns();
        }
    }

    final class InternalHaving<T> extends Having<T> {
        InternalHaving(@NotNull Class<T> clazz) {
            super(clazz);
        }

        InternalHaving(@NotNull Class<T> clazz, List<Criteria> criteria) {
            super(clazz);
            this.criteria.addAll(criteria);
        }

        @Override
        protected Having<T> newInstance() {
            return new InternalHaving<>(clazz);
        }

        @Override
        protected char namedParamPrefix() {
            return namedParamPrefix;
        }

        public Pair<String, Map<String, Object>> getHavingClause() {
            return build();
        }

        private List<Criteria> getCriteria() {
            return criteria;
        }
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
            SqlGenerator.GeneratedSqlMetaData result = prepareSql(recordQuery, args);
            String sourceQuery = result.getSourceSql();
            Map<String, Object> myArgs = result.getArgs();
            if (count == null) {
                String cq = countQuery == null ? sqlGenerator.generateCountQuery(sourceQuery) : countQuery;
                try (Stream<DataRow> s = executeQueryStream(cq, myArgs)) {
                    count = s.findFirst()
                            .map(d -> d.getInt(0))
                            .orElse(0);
                }
            }

            PageHelper pageHelper = null;

            if (pageHelperProvider != null) {
                pageHelper = pageHelperProvider.customPageHelper(metaData, databaseId, namedParamPrefix);
            }

            if (pageHelper == null) {
                pageHelper = builtinPager();
            }
            pageHelper.init(page, size, count);
            Args<Integer> pagedArgs = pageHelper.pagedArgs();
            myArgs.putAll(rewriteArgsFunc == null ? pagedArgs : rewriteArgsFunc.apply(pagedArgs));
            String executeQuery = disablePageSql ? sourceQuery : pageHelper.pagedSql(namedParamPrefix, sourceQuery);
            final PageHelper finalPageHelper = pageHelper;
            try (Stream<DataRow> s = executeQueryStream(executeQuery, myArgs)) {
                List<T> list = s.peek(d -> d.remove(PageHelper.ROW_NUM_KEY))
                        .map(mapper)
                        .collect(Collectors.toList());
                return PagedResource.of(finalPageHelper, list);
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
            case "microsoft sql server":
                return new SqlServer2012PageHelper();
            default:
                throw new UnsupportedOperationException("pager of \"" + databaseId + "\" default not implement currently, see method 'setGlobalPageHelperProvider'.");
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
    protected SqlGenerator.GeneratedSqlMetaData prepareSql(@NotNull String sql, Map<String, ?> args) {
        Map<String, Object> myArgs = new HashMap<>();
        if (Objects.nonNull(args)) {
            myArgs.putAll(args);
        }
        String mySql = SqlUtil.trimEnd(sql.trim());
        if (Objects.nonNull(sqlInterceptor)) {
            sqlInterceptor.preHandle(mySql, myArgs, metaData);
        }
        if (mySql.startsWith("&")) {
            log.debug("SQL name: {}", mySql);
            if (Objects.nonNull(xqlFileManager)) {
                Pair<String, Map<String, Object>> result = xqlFileManager.get(mySql.substring(1), myArgs);
                mySql = result.getItem1();
                // #for expression temp variables stored in _for variable.
                if (!result.getItem2().isEmpty()) {
                    myArgs.put(XQLFileManager.DynamicSqlParser.FOR_VARS_KEY, result.getItem2());
                }
            } else {
                throw new NullPointerException("can not find property 'xqlFileManager'.");
            }
        }
        if (Objects.nonNull(sqlParseChecker)) {
            mySql = sqlParseChecker.handle(mySql, myArgs);
        }
        if (mySql.contains("${")) {
            mySql = SqlUtil.formatSql(mySql, myArgs, sqlGenerator.getTemplateFormatter());
            if (Objects.nonNull(xqlFileManager)) {
                mySql = SqlUtil.formatSql(mySql, xqlFileManager.getConstants(), sqlGenerator.getTemplateFormatter());
            }
        }
        log.debug("SQL: {}", SqlHighlighter.highlightIfAnsiCapable(mySql));
        log.debug("Args: {}", myArgs);
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
            throw new ConnectionStatusException("fetch connection failed.", e);
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

    public void setSqlInterceptor(SqlInterceptor sqlInterceptor) {
        this.sqlInterceptor = sqlInterceptor;
    }

    public void setStatementValueHandler(StatementValueHandler statementValueHandler) {
        if (Objects.nonNull(statementValueHandler))
            this.statementValueHandler = statementValueHandler;
    }

    public void setXqlFileManager(XQLFileManager xqlFileManager) {
        if (Objects.nonNull(xqlFileManager)) {
            this.xqlFileManager = xqlFileManager;
            this.xqlFileManager.setDatabaseId(databaseId);
            this.xqlFileManager.setTemplateFormatter(sqlGenerator.getTemplateFormatter());
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

    public void setSqlWatcher(SqlWatcher sqlWatcher) {
        this.sqlWatcher = sqlWatcher;
    }

    public void setQueryTimeoutHandler(QueryTimeoutHandler queryTimeoutHandler) {
        if (Objects.nonNull(queryTimeoutHandler)) {
            this.queryTimeoutHandler = queryTimeoutHandler;
        }
    }

    public Map<SqlStatementType, SqlInvokeHandler> getXqlMappingHandlers() {
        return xqlMappingHandlers;
    }

    public void registerXqlMappingHandler(SqlStatementType type, SqlInvokeHandler handler) {
        xqlMappingHandlers.put(type, handler);
    }

    public QueryCacheManager getQueryCacheManager() {
        return queryCacheManager;
    }

    public void setQueryCacheManager(QueryCacheManager queryCacheManager) {
        this.queryCacheManager = queryCacheManager;
    }

    /**
     * Could be cleared if necessary.
     *
     * @return query cache locks object.
     */
    public Map<String, Object> getQueryCacheLocks() {
        return queryCacheLocks;
    }

    public void setSqlParseChecker(SqlParseChecker sqlParseChecker) {
        this.sqlParseChecker = sqlParseChecker;
    }

    public Set<String> getOperatorWhiteList() {
        return operatorWhiteList;
    }

    public void setOperatorWhiteList(Set<String> operatorWhiteList) {
        this.operatorWhiteList = operatorWhiteList;
    }
}
