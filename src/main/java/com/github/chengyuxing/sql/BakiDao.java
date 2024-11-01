package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.sql.datasource.DataSourceUtil;
import com.github.chengyuxing.sql.dsl.*;
import com.github.chengyuxing.sql.dsl.clause.GroupBy;
import com.github.chengyuxing.sql.dsl.clause.Having;
import com.github.chengyuxing.sql.dsl.clause.OrderBy;
import com.github.chengyuxing.sql.dsl.clause.Where;
import com.github.chengyuxing.sql.dsl.clause.condition.Criteria;
import com.github.chengyuxing.sql.dsl.type.FieldReference;
import com.github.chengyuxing.sql.dsl.type.OrderByType;
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
import com.github.chengyuxing.sql.plugins.*;
import com.github.chengyuxing.sql.support.*;
import com.github.chengyuxing.sql.support.executor.Executor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.annotation.Type;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
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
    private final Map<Type, SqlInvokeHandler> xqlMappingHandlers = new HashMap<>();
    private final Map<String, Object> queryCacheLocks = new ConcurrentHashMap<>();
    private final DataSource dataSource;
    private DatabaseMetaData metaData;
    private String databaseId;
    private SqlGenerator sqlGenerator;
    private EntityManager entityManager;

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
     * Query cache manager.
     */
    private QueryCacheManager queryCacheManager;
    /**
     * Query dsl condition operator white list.
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

    /**
     * Caches the query result if necessary.
     *
     * @param key  cache key
     * @param sql  sql name or sql string
     * @param args args
     * @return query result
     */
    protected Stream<DataRow> executeQueryStreamWithCache(String key, String sql, Map<String, Object> args) {
        if (Objects.isNull(queryCacheManager) || !queryCacheManager.isAvailable(key, args)) {
            return executeQueryStream(sql, args);
        }
        String uniqueKey = queryCacheManager.uniqueKey(key, args);
        Stream<DataRow> cache = queryCacheManager.get(uniqueKey);
        if (Objects.nonNull(cache)) {
            log.debug("Hits cache({}, {}), returns data from cache.", key, args);
            return cache;
        }
        Object lock = queryCacheLocks.computeIfAbsent(uniqueKey, k -> new Object());
        synchronized (lock) {
            cache = queryCacheManager.get(uniqueKey);
            if (Objects.nonNull(cache)) {
                log.debug("Hits cache({}, {}) after lock, returns data from cache.", key, args);
                return cache;
            }
            List<DataRow> prepareCache = new ArrayList<>();
            Stream<DataRow> queryStream = executeQueryStream(sql, args)
                    .peek(prepareCache::add)
                    .onClose(() -> queryCacheManager.put(uniqueKey, prepareCache));
            log.debug("Put query result({}, {}) to cache.", key, args);
            return queryStream;
        }
    }

    @Override
    public QueryExecutor query(@NotNull String sql) {
        return new QueryExecutor(sql) {
            @Override
            public Stream<DataRow> stream() {
                return watchSql(sql, sql, args, () -> executeQueryStreamWithCache(sql, sql, args));
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
            public @NotNull DataRow zip() {
                return DataRow.zip(rows());
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

            @Override
            public boolean exists() {
                return findFirst().isPresent();
            }
        };
    }

    @Override
    public <T, SELF extends Query<T, SELF>> Query<T, SELF> query(@NotNull Class<T> clazz) {
        EntityManager.EntityMeta entityMeta = entityManager.getEntityMeta(clazz);
        return new Query<T, SELF>() {
            private final Set<String> selectColumns = new LinkedHashSet<>();
            private final List<Criteria> finalWhereCriteria = new ArrayList<>();
            private final Set<Pair<String, OrderByType>> finalOrderBy = new LinkedHashSet<>();
            private final Set<String> finalGroupByAggColumns = new LinkedHashSet<>();
            private final Set<String> finalGroupByColumns = new LinkedHashSet<>();
            private final List<Criteria> finalHavingCriteria = new ArrayList<>();

            private Triple<String, String, Map<String, Object>> createQuery() {
                String select = selectColumns.isEmpty() ? entityMeta.getSelect() : entityMeta.getSelect(selectColumns);
                String countSelect = entityMeta.getCountSelect();

                Pair<String, Map<String, Object>> where = new InternalWhere<>(clazz, finalWhereCriteria).getWhere();
                if (!where.getItem1().isEmpty()) {
                    select += where.getItem1();
                    countSelect += where.getItem1();
                }
                String orderBy = new InternalOrderBy<>(clazz, finalOrderBy).getOrderBy();
                if (!orderBy.isEmpty()) {
                    select += orderBy;
                }
                return Triple.of(select, countSelect, where.getItem2());
            }

            private InternalGroupBy<T> createGroupBy() {
                Pair<String, Map<String, Object>> where = new InternalWhere<>(clazz, finalWhereCriteria).getWhere();
                String orderBy = new InternalOrderBy<>(clazz, finalOrderBy).getOrderBy();
                InternalGroupBy<T> groupBy = new InternalGroupBy<>(clazz, finalGroupByAggColumns, finalGroupByColumns, finalHavingCriteria);
                groupBy.setWhereClause(where.getItem1());
                groupBy.setOrderByClause(orderBy);
                groupBy.setArgs(where.getItem2());
                return groupBy;
            }

            @Override
            public SELF where(@NotNull Function<Where<T>, Where<T>> where) {
                Where<T> gotten = where.apply(new InternalWhere<>(clazz));
                InternalWhere<T> wrapper = new InternalWhere<>(clazz, gotten);
                finalWhereCriteria.addAll(wrapper.getCriteria());
                //noinspection unchecked
                return (SELF) this;
            }

            @Override
            public SELF groupBy(@NotNull Function<GroupBy<T>, GroupBy<T>> groupBy) {
                GroupBy<T> gotten = groupBy.apply(new InternalGroupBy<>(clazz));
                InternalGroupBy<T> wrapper = new InternalGroupBy<>(clazz, gotten);
                finalGroupByAggColumns.addAll(wrapper.getAggColumns());
                finalGroupByColumns.addAll(wrapper.getGroupColumns());
                finalHavingCriteria.addAll(wrapper.getHavingCriteria());
                //noinspection unchecked
                return (SELF) this;
            }

            @Override
            public SELF orderBy(@NotNull Function<OrderBy<T>, OrderBy<T>> orderBy) {
                OrderBy<T> gotten = orderBy.apply(new InternalOrderBy<>(clazz));
                InternalOrderBy<T> wrapper = new InternalOrderBy<>(clazz, gotten);
                finalOrderBy.addAll(wrapper.getOrders());
                //noinspection unchecked
                return (SELF) this;
            }

            @Override
            public SELF select(@NotNull List<FieldReference<T>> columns) {
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
            public SELF peek(@NotNull BiConsumer<String, Pair<String, Map<String, Object>>> consumer) {
                Triple<String, String, Map<String, Object>> query = createQuery();
                consumer.accept(query.getItem1(), Pair.of(query.getItem2(), Collections.unmodifiableMap(query.getItem3())));
                //noinspection unchecked
                return (SELF) this;
            }

            @Override
            public Stream<DataRow> toRowStream() {
                // there is no any group by columns, just execute query without group by clause.
                if (finalGroupByColumns.isEmpty()) {
                    if (!finalGroupByAggColumns.isEmpty()) {
                        throw new IllegalStateException("group by clause must have at least one column");
                    }
                    Triple<String, String, Map<String, Object>> query = createQuery();
                    String key = entityCallKey(clazz, query.getItem1());
                    return watchSql(key, query.getItem1(), query.getItem3(),
                            () -> executeQueryStreamWithCache(
                                    key,
                                    query.getItem1(),
                                    query.getItem3()
                            ));
                }
                return createGroupBy().query();
            }

            @Override
            public Stream<T> toStream() {
                return toRowStream().map(d -> EntityUtil.mapToEntity(d, clazz));
            }

            @Override
            public List<T> toList() {
                try (Stream<T> s = toStream()) {
                    return s.collect(Collectors.toList());
                }
            }

            @Override
            public <R> List<R> toList(@NotNull Function<T, R> mapper) {
                try (Stream<T> s = toStream()) {
                    return s.map(mapper).collect(Collectors.toList());
                }
            }

            @Override
            public <R, V> R collect(@NotNull Function<T, V> mapper, @NotNull Collector<V, ?, R> collector) {
                try (Stream<T> s = toStream()) {
                    return s.map(mapper).collect(collector);
                }
            }

            @Override
            public <R> R collect(@NotNull Collector<T, ?, R> collector) {
                try (Stream<T> s = toStream()) {
                    return s.collect(collector);
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
                Triple<String, String, Map<String, Object>> query = createQuery();
                return new SimplePageable(query.getItem1(), page, size)
                        .args(query.getItem3())
                        .pageHelper(pageHelperProvider)
                        .count(query.getItem2())
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
                String query;
                String countQuery;
                Map<String, Object> args;
                if (finalGroupByColumns.isEmpty()) {
                    if (!finalGroupByAggColumns.isEmpty()) {
                        throw new IllegalStateException("group by clause must have at least one column");
                    }
                    Triple<String, String, Map<String, Object>> queryObj = createQuery();
                    query = queryObj.getItem1();
                    countQuery = queryObj.getItem2();
                    args = queryObj.getItem3();
                } else {
                    // group by paged query
                    InternalGroupBy<T> groupBy = createGroupBy();

                    Pair<String, Map<String, Object>> querySqlObj = groupBy.getQuerySql();
                    query = querySqlObj.getItem1();
                    args = querySqlObj.getItem2();

                    countQuery = sqlGenerator.generateCountQuery(
                            entityMeta.getSelect(groupBy.getGroupColumns()) +
                                    groupBy.getWhereClause() +
                                    groupBy.getGroupByClause() +
                                    groupBy.getHavingClause().getItem1()
                    );
                }
                return new SimplePageable(query, page, size)
                        .args(args)
                        .pageHelper(pageHelperProvider)
                        .count(countQuery)
                        .collect();
            }

            @Override
            public @NotNull PagedResource<DataRow> toPagedRowResource(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                                                                      @Range(from = 1, to = Integer.MAX_VALUE) int size) {
                return toPagedRowResource(page, size, null);
            }

            @Override
            public boolean exists() {
                String query = entityMeta.getExistsSelect();
                Pair<String, Map<String, Object>> where = new InternalWhere<>(clazz, finalWhereCriteria).getWhere();
                if (where.getItem1().isEmpty()) {
                    throw new IllegalSqlException("Exists query must have condition.");
                }
                query += where.getItem1();
                final String existQuery = query;
                String key = entityCallKey(clazz, existQuery);
                return watchSql(key, existQuery, where.getItem2(), () -> {
                    try (Stream<DataRow> s = executeQueryStream(existQuery, where.getItem2())) {
                        return s.findFirst().isPresent();
                    }
                });
            }

            @Override
            public @Range(from = 0, to = Long.MAX_VALUE) long count() {
                String countSelect;
                Map<String, Object> args = Collections.emptyMap();
                if (finalGroupByColumns.isEmpty()) {
                    if (!finalGroupByAggColumns.isEmpty()) {
                        throw new IllegalStateException("group by clause must have at least one column");
                    }
                    countSelect = entityMeta.getCountSelect();

                    Pair<String, Map<String, Object>> where = new InternalWhere<>(clazz, finalWhereCriteria).getWhere();
                    if (!where.getItem1().isEmpty()) {
                        countSelect += where.getItem1();
                        args = where.getItem2();
                    }
                } else {
                    InternalGroupBy<T> groupBy = createGroupBy();

                    countSelect = sqlGenerator.generateCountQuery(
                            entityMeta.getSelect(groupBy.getGroupColumns()) +
                                    groupBy.getWhereClause() +
                                    groupBy.getGroupByClause() +
                                    groupBy.getHavingClause().getItem1()
                    );
                    args = groupBy.getQuerySql().getItem2();
                }
                final String countQuery = countSelect;
                final Map<String, Object> myArgs = args;
                String key = entityCallKey(clazz, countQuery);
                return watchSql(key, countQuery, myArgs, () -> {
                    try (Stream<DataRow> s = executeQueryStreamWithCache(key, countQuery, myArgs)) {
                        return s.findFirst()
                                .map(d -> d.getLong(0))
                                .orElse(0L);
                    }
                });
            }

            @Override
            public <R, V> R collectRow(@NotNull Function<DataRow, V> mapper, @NotNull Collector<V, ?, R> collector) {
                try (Stream<DataRow> s = toRowStream()) {
                    return s.map(mapper).collect(collector);
                }
            }

            @Override
            public <R> R collectRow(@NotNull Collector<DataRow, ?, R> collector) {
                try (Stream<DataRow> s = toRowStream()) {
                    return s.collect(collector);
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
            public List<DataRow> toRows() {
                try (Stream<DataRow> s = toRowStream()) {
                    return s.collect(Collectors.toList());
                }
            }

            @Override
            public List<Map<String, Object>> toMaps() {
                try (Stream<DataRow> s = toRowStream()) {
                    return s.collect(Collectors.toList());
                }
            }

            @Override
            public @NotNull Pair<String, Map<String, Object>> getSql() {
                Triple<String, String, Map<String, Object>> query = createQuery();
                return Pair.of(query.getItem2(), Collections.unmodifiableMap(query.getItem3()));
            }
        };
    }

    @Override
    public <T> int insert(@NotNull T entity) {
        @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) entity.getClass();
        String insert = entityManager.getEntityMeta(clazz).getInsert();
        Map<String, Object> data = Args.ofEntity(entity);
        String key = entityCallKey(clazz, insert);
        return watchSql(key, insert, data, () -> executeUpdate(insert, data));
    }

    @Override
    public <T> int insert(@NotNull Collection<T> entities) {
        if (entities.isEmpty()) return 0;
        @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) entities.iterator().next().getClass();
        String insert = entityManager.getEntityMeta(clazz).getInsert();
        List<Map<String, Object>> data = entities.stream()
                .map(Args::ofEntity)
                .collect(Collectors.toList());
        String key = entityCallKey(clazz, insert);
        return watchSql(key, insert, data, () -> executeBatchUpdate(insert, data, batchSize));
    }

    @Override
    public <T> Update<T> update(@NotNull T entity) {
        @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) entity.getClass();
        EntityManager.EntityMeta entityMeta = entityManager.getEntityMeta(clazz);

        return new Update<T>() {
            private String dynamicUpdate(Map<String, Object> data, String where) {
                return sqlGenerator.generateNamedParamUpdate(
                        entityManager.getTableName(clazz),
                        entityMeta.getColumns().keySet(),
                        data,
                        true
                ) + where;
            }

            @Override
            public int byId() {
                String idColumn = entityMeta.getPrimaryKey();
                String whereById = "\nwhere " + idColumn + " = " + namedParamPrefix + idColumn;
                Map<String, Object> data = Args.ofEntity(entity);
                String update = ignoreNull ? dynamicUpdate(data, whereById) : entityMeta.getUpdate() + whereById;
                String key = entityCallKey(clazz, update);
                return watchSql(key, update, data, () -> executeUpdate(update, data));
            }

            @Override
            public int by(Function<Where<T>, Where<T>> where) {
                Where<T> gotten = where.apply(new InternalWhere<>(clazz));
                InternalWhere<T> wrapper = new InternalWhere<>(clazz, gotten);

                Map<String, Object> data = Args.ofEntity(entity);
                Pair<String, Map<String, Object>> whereClause = wrapper.getWhere();

                if (whereClause.getItem1().isEmpty()) {
                    throw new IllegalSqlException("Update must have condition.");
                }
                String whereBy = whereClause.getItem1();
                String update = ignoreNull ? dynamicUpdate(data, whereBy) : entityMeta.getUpdate() + whereBy;
                data.putAll(whereClause.getItem2());
                String key = entityCallKey(clazz, update);
                return watchSql(key, update, data, () -> executeUpdate(update, data));
            }
        };
    }

    @Override
    public <T> Delete<T> delete(@NotNull T entity) {
        @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) entity.getClass();
        EntityManager.EntityMeta entityMeta = entityManager.getEntityMeta(clazz);
        return new Delete<T>() {
            @Override
            public int byId() {
                String idColumn = entityMeta.getPrimaryKey();
                String delete = entityMeta.getDelete() + "\nwhere " + idColumn + " = " + namedParamPrefix + idColumn;
                Map<String, Object> data = Args.ofEntity(entity);
                String key = entityCallKey(clazz, delete);
                return watchSql(key, delete, data, () -> executeUpdate(delete, data));
            }

            @Override
            public int by(Function<Where<T>, Where<T>> where) {
                Where<T> gotten = where.apply(new InternalWhere<>(clazz));
                InternalWhere<T> wrapper = new InternalWhere<>(clazz, gotten);

                Pair<String, Map<String, Object>> whereClause = wrapper.getWhere();
                if (whereClause.getItem1().isEmpty()) {
                    throw new IllegalSqlException("Delete must have condition.");
                }
                String delete = entityMeta.getDelete() + whereClause.getItem1();
                Map<String, Object> data = Args.ofEntity(entity);
                data.putAll(whereClause.getItem2());
                String key = entityCallKey(clazz, delete);
                return watchSql(key, delete, data, () -> executeUpdate(delete, data));
            }
        };
    }

    @Override
    public Executor of(@NotNull String sql) {
        return new Executor() {
            @Override
            public @NotNull DataRow execute() {
                return watchSql(sql, sql, Collections.emptyMap(), () -> BakiDao.super.execute(sql, Collections.emptyMap()));
            }

            @Override
            public @NotNull DataRow execute(Map<String, ?> args) {
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
            public @NotNull DataRow call(Map<String, Param> params) {
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
        if (Objects.isNull(sqlWatcher)) {
            return supplier.get();
        }
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
    public @NotNull String databaseId() {
        return this.databaseId;
    }

    final class InternalWhere<T> extends Where<T> {
        private final Class<T> clazz;
        private final Map<String, EntityManager.ColumnMeta> columns;

        InternalWhere(Class<T> clazz) {
            super(clazz);
            this.clazz = clazz;
            this.columns = entityManager.getColumns(this.clazz);
        }

        InternalWhere(Class<T> clazz, Where<T> other) {
            super(clazz, other);
            this.clazz = clazz;
            this.columns = entityManager.getColumns(clazz);
        }

        InternalWhere(Class<T> clazz, List<Criteria> criteria) {
            super(clazz);
            this.clazz = clazz;
            this.columns = entityManager.getColumns(clazz);
            this.criteria = criteria;
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
            return columns.keySet();
        }

        @Override
        protected Set<String> operatorWhiteList() {
            return operatorWhiteList;
        }

        private Pair<String, Map<String, Object>> getWhere() {
            return build();
        }

        private List<Criteria> getCriteria() {
            return criteria;
        }
    }

    final class InternalOrderBy<T> extends OrderBy<T> {
        private final Map<String, EntityManager.ColumnMeta> columns;

        InternalOrderBy(Class<T> clazz) {
            super(clazz);
            this.columns = entityManager.getColumns(clazz);
        }

        InternalOrderBy(Class<T> clazz, OrderBy<T> other) {
            super(clazz, other);
            this.columns = entityManager.getColumns(clazz);
        }

        InternalOrderBy(Class<T> clazz, Set<Pair<String, OrderByType>> orders) {
            super(clazz);
            this.columns = entityManager.getColumns(clazz);
            this.orders = orders;
        }

        private Set<Pair<String, OrderByType>> getOrders() {
            return orders;
        }

        private String getOrderBy() {
            return build();
        }

        @Override
        protected Set<String> columnWhiteList() {
            return columns.keySet();
        }

        @Override
        protected Set<String> operatorWhiteList() {
            return operatorWhiteList;
        }
    }

    final class InternalGroupBy<T> extends GroupBy<T> {
        private final Map<String, EntityManager.ColumnMeta> columns;
        private String whereClause = "";
        private String orderByClause = "";
        private Map<String, Object> args = Collections.emptyMap();

        InternalGroupBy(@NotNull Class<T> clazz) {
            super(clazz);
            this.columns = entityManager.getColumns(clazz);
        }

        InternalGroupBy(@NotNull Class<T> clazz, Set<String> aggColumns, Set<String> groupColumns, List<Criteria> havingCriteria) {
            super(clazz);
            this.columns = entityManager.getColumns(clazz);
            this.groupColumns = groupColumns;
            this.aggColumns = aggColumns;
            this.havingCriteria = havingCriteria;
        }

        InternalGroupBy(@NotNull Class<T> clazz, GroupBy<T> other) {
            super(clazz, other);
            this.columns = entityManager.getColumns(clazz);
        }

        @Override
        public GroupBy<T> having(Function<Having<T>, Having<T>> having) {
            Having<T> gotten = having.apply(new InternalHaving<>(clazz));
            InternalHaving<T> wrapper = new InternalHaving<>(clazz, gotten);
            havingCriteria.addAll(wrapper.getCriteria());
            return this;
        }

        @Override
        protected Stream<DataRow> query() {
            Pair<String, Map<String, Object>> query = getQuerySql();
            String key = entityCallKey(clazz, query.getItem1());
            return watchSql(key, query.getItem1(), query.getItem2(),
                    () -> executeQueryStreamWithCache(key, query.getItem1(), query.getItem2()));
        }

        @Override
        protected Set<String> columnWhiteList() {
            return columns.keySet();
        }

        @Override
        protected Set<String> operatorWhiteList() {
            return operatorWhiteList;
        }

        private Pair<String, Map<String, Object>> getQuerySql() {
            Map<String, Object> allArgs = new HashMap<>();
            String query = entityManager.getEntityMeta(clazz).getSelect(getSelectColumns());
            if (!whereClause.isEmpty()) {
                query += whereClause;
            }

            String groupByClause = buildGroupByClause();
            if (!groupByClause.isEmpty()) {
                query += groupByClause;
            }

            Pair<String, Map<String, Object>> having = new InternalHaving<>(clazz, havingCriteria).getHavingClause();
            if (!having.getItem1().isEmpty()) {
                query += having.getItem1();
            }

            if (!orderByClause.isEmpty()) {
                query += orderByClause;
            }
            allArgs.putAll(args);
            allArgs.putAll(having.getItem2());
            return Pair.of(query, allArgs);
        }

        private Pair<String, Map<String, Object>> getHavingClause() {
            return new InternalHaving<>(clazz, havingCriteria).getHavingClause();
        }

        private List<Criteria> getHavingCriteria() {
            return havingCriteria;
        }

        public Map<String, Object> getArgs() {
            return args;
        }

        public String getWhereClause() {
            return whereClause;
        }

        public String getOrderByClause() {
            return orderByClause;
        }

        public String getGroupByClause() {
            return buildGroupByClause();
        }

        private void setWhereClause(String whereClause) {
            this.whereClause = whereClause;
        }

        private void setOrderByClause(String orderByClause) {
            this.orderByClause = orderByClause;
        }

        private void setArgs(Map<String, Object> args) {
            this.args = args;
        }

        private Set<String> getGroupColumns() {
            return groupColumns;
        }

        private Set<String> getAggColumns() {
            return aggColumns;
        }
    }

    final class InternalHaving<T> extends Having<T> {

        InternalHaving(@NotNull Class<T> clazz) {
            super(clazz);
        }

        InternalHaving(@NotNull Class<T> clazz, Having<T> other) {
            super(clazz, other);
        }

        InternalHaving(@NotNull Class<T> clazz, List<Criteria> criteria) {
            super(clazz);
            this.criteria = criteria;
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
            Pair<String, Map<String, Object>> result = parseSql(recordQuery, args);
            String query = result.getItem1();
            Map<String, Object> myArgs = result.getItem2();
            if (count == null) {
                String cq = countQuery == null ? sqlGenerator.generateCountQuery(query) : countQuery;
                count = watchSql(recordQuery, cq, myArgs, () -> {
                    try (Stream<DataRow> s = executeQueryStreamWithCache(recordQuery, cq, myArgs)) {
                        return s.findFirst()
                                .map(d -> d.getInt(0))
                                .orElse(0);
                    }
                });
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
                try (Stream<DataRow> s = executeQueryStreamWithCache(recordQuery, executeQuery, myArgs)) {
                    List<T> list = s.peek(d -> d.remove(PageHelper.ROW_NUM_KEY))
                            .map(mapper)
                            .collect(Collectors.toList());
                    return PagedResource.of(finalPageHelper, list);
                }
            });
        }
    }

    protected String entityCallKey(Class<?> clazz, String sql) {
        return "@" + clazz.getName() + ":" + sql;
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
        String mySql = SqlUtil.trimEnd(sql.trim());
        if (mySql.startsWith("&")) {
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
        if (mySql.contains("${")) {
            mySql = SqlUtil.formatSql(mySql, myArgs, sqlGenerator.getTemplateFormatter());
            if (Objects.nonNull(xqlFileManager)) {
                mySql = SqlUtil.formatSql(mySql, xqlFileManager.getConstants(), sqlGenerator.getTemplateFormatter());
            }
        }
        if (Objects.nonNull(sqlParseChecker)) {
            mySql = sqlParseChecker.handle(mySql);
        }
        if (Objects.nonNull(sqlInterceptor)) {
            boolean request = sqlInterceptor.preHandle(mySql, myArgs, metaData);
            if (!request) {
                throw new IllegalSqlException("permission denied, reject to execute sql.\nSQL: " + mySql + "\nArgs: " + myArgs);
            }
        }
        return Pair.of(mySql, myArgs);
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

    public boolean isAutoXFMConfig() {
        return autoXFMConfig;
    }

    public void setAutoXFMConfig(boolean autoXFMConfig) {
        this.autoXFMConfig = autoXFMConfig;
        if (this.autoXFMConfig) {
            loadXFMConfigByDatabaseId();
        }
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
        this.sqlWatcher = sqlWatcher;
    }

    public void setQueryTimeoutHandler(QueryTimeoutHandler queryTimeoutHandler) {
        if (Objects.nonNull(queryTimeoutHandler)) {
            this.queryTimeoutHandler = queryTimeoutHandler;
        }
    }

    public Map<Type, SqlInvokeHandler> getXqlMappingHandlers() {
        return xqlMappingHandlers;
    }

    public void registerXqlMappingHandler(Type type, SqlInvokeHandler handler) {
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
