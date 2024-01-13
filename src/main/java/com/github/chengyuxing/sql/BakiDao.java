package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
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
import com.github.chengyuxing.sql.support.AfterParseDynamicSql;
import com.github.chengyuxing.sql.support.JdbcSupport;
import com.github.chengyuxing.sql.support.SqlInterceptor;
import com.github.chengyuxing.sql.support.StatementValueHandler;
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
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <h2>Default implementation of Baki interface</h2>
 * <p>If {@link XQLFileManager } configured, all methods will be support replace sql statement to sql name ({@code &<alias>.<sqlName>}).</p>
 * Example:
 * <blockquote>
 * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; s = baki.query("&amp;sys.getUser").stream()) {
 *     s.forEach(System.out::println);
 *   }</pre>
 * </blockquote>
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final DataSource dataSource;
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
        this.sqlGenerator = new SqlGenerator(namedParamPrefix);
        this.statementValueHandler = (ps, index, value, metaData) -> JdbcUtil.setStatementValue(ps, index, value);
        this.afterParseDynamicSql = SqlUtil::repairSyntaxError;
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

    @Override
    public QueryExecutor query(String sql) {
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
            public DataRow findFirstRow() {
                return findFirst().orElseGet(() -> new DataRow(0));
            }

            @Override
            public <T> T findFirstEntity(Class<T> entityClass) {
                return findFirst().map(d -> d.toEntity(entityClass)).orElse(null);
            }

            @Override
            public Optional<DataRow> findFirst() {
                PageHelper pageHelper = defaultPager();
                pageHelper.init(1, 1, 1);
                Map<String, Integer> pagedArgs = pageHelper.pagedArgs();
                if (Objects.nonNull(pagedArgs)) {
                    args.putAll(pagedArgs);
                }
                Pair<String, Map<String, Object>> result = parseSql(sql, args);
                String pagedSql = pageHelper.pagedSql(result.getItem1());
                try (Stream<DataRow> s = executeQueryStream(pagedSql, result.getItem2())) {
                    return s.peek(d -> d.remove(PageHelper.ROW_NUM_KEY)).findFirst();
                }
            }

            @Override
            public boolean exists() {
                return findFirst().isPresent();
            }
        };
    }

    @Override
    public SaveExecutor update(String tableName, String where) {
        return new SaveExecutor() {
            @Override
            public int save(Map<String, ?> data) {
                return save(Collections.singletonList(data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                if (data.isEmpty()) {
                    return 0;
                }
                String whereStatement = parseSql(where, Collections.emptyMap()).getItem1();
                List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
                if (fast) {
                    String update = sqlGenerator.generateNamedParamUpdate(tableName,
                            whereStatement,
                            data.iterator().next(),
                            tableFields,
                            ignoreNull);
                    return executeBatchUpdate(update, data, batchSize);
                }
                int i = 0;
                for (Map<String, ?> item : data) {
                    String update = sqlGenerator.generateNamedParamUpdate(tableName,
                            whereStatement,
                            item,
                            tableFields,
                            ignoreNull);
                    i += executeUpdate(update, item);
                }
                return i;
            }
        };
    }

    @Override
    public SaveExecutor insert(String tableName) {
        return new SaveExecutor() {
            @Override
            public int save(Map<String, ?> data) {
                return save(Collections.singletonList(data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                if (data.isEmpty()) {
                    return 0;
                }
                List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
                if (fast) {
                    String insert = sqlGenerator.generateNamedParamInsert(tableName, data.iterator().next(), tableFields, ignoreNull);
                    return executeBatchUpdate(insert, data, batchSize);
                }
                int i = 0;
                for (Map<String, ?> item : data) {
                    String insert = sqlGenerator.generateNamedParamInsert(tableName, item, tableFields, ignoreNull);
                    i += executeUpdate(insert, item);
                }
                return i;
            }
        };
    }

    @Override
    public SaveExecutor delete(String tableName, String where) {
        return new SaveExecutor() {
            @Override
            public int save(Map<String, ?> data) {
                return save(Collections.singletonList(data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                String whereSql = parseSql(where, Collections.emptyMap()).getItem1();
                String w = StringUtil.startsWithIgnoreCase(whereSql.trim(), "where") ? whereSql : "\nwhere " + whereSql;
                String delete = "delete from " + tableName + w;
                if (data.isEmpty()) {
                    return executeUpdate(delete, Collections.emptyMap());
                }
                if (fast) {
                    return executeBatchUpdate(delete, data, batchSize);
                }
                int i = 0;
                for (Map<String, ?> item : data) {
                    i += executeUpdate(delete, item);
                }
                return i;
            }
        };
    }

    @Override
    public Executor of(String sql) {
        return new Executor() {
            @Override
            public DataRow execute() {
                return BakiDao.super.execute(sql, Collections.emptyMap());
            }

            @Override
            public DataRow execute(Map<String, ?> args) {
                return BakiDao.super.execute(sql, args);
            }

            @Override
            public int executeBatch(String... moreSql) {
                List<String> sqlList = new ArrayList<>(Arrays.asList(moreSql));
                sqlList.add(0, sql);
                return BakiDao.super.executeBatch(sqlList, batchSize);
            }

            @Override
            public int executeBatch(Collection<? extends Map<String, ?>> data) {
                Map<String, ?> arg = data.isEmpty() ? new HashMap<>() : data.iterator().next();
                Pair<String, Map<String, Object>> parsed = parseSql(sql, arg);
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
            }

            @Override
            public DataRow call(Map<String, Param> params) {
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
    public DatabaseMetaData metaData() {
        return this.metaData;
    }

    @Override
    public String databaseId() {
        return this.databaseId;
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
            Map<String, Object> data = result.getItem2();
            if (count == null) {
                String cq = countQuery;
                if (cq == null) {
                    cq = sqlGenerator.generateCountQuery(query);
                }
                List<DataRow> cnRows = execute(cq, data).getFirstAs();
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
            data.putAll(rewriteArgsFunc == null ? pagedArgs : rewriteArgsFunc.apply(pagedArgs));
            String executeQuery = disablePageSql ? query : pageHelper.pagedSql(query);
            try (Stream<DataRow> s = executeQueryStream(executeQuery, data)) {
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
                throw new UnsupportedOperationException("pager of \"" + databaseId + "\" default not implement currently, see method 'setPageHelperProvider'.");
        }
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
        Map<String, Object> data = new HashMap<>();
        if (Objects.nonNull(args)) {
            data.putAll(args);
        }
        String trimSql = SqlUtil.trimEnd(sql.trim());
        if (trimSql.startsWith("&")) {
            if (Objects.nonNull(xqlFileManager)) {
                if (reloadXqlOnGet) {
                    log.warn("please set 'reloadXqlOnGet' to false in production environment for improve concurrency.");
                    xqlFileManager.init();
                }
                Pair<String, Map<String, Object>> result = xqlFileManager.get(trimSql.substring(1), data);
                trimSql = afterParseDynamicSql.handle(result.getItem1());
                // #for expression temp variables stored in _for variable.
                if (!result.getItem2().isEmpty()) {
                    data.put(XQLFileManager.DynamicSqlParser.FOR_VARS_KEY, result.getItem2());
                }
            } else {
                throw new NullPointerException("can not find property 'xqlFileManager' or XQLFileManager object init failed!");
            }
        }
        if (trimSql.contains("${")) {
            trimSql = SqlUtil.formatSql(trimSql, data);
            if (Objects.nonNull(xqlFileManager)) {
                trimSql = SqlUtil.formatSql(trimSql, xqlFileManager.getConstants());
            }
        }
        if (Objects.nonNull(sqlInterceptor)) {
            boolean request = sqlInterceptor.preHandle(trimSql, data, metaData);
            if (!request) {
                throw new IllegalSqlException("permission denied, reject to execute invalid sql.\nSQL: " + trimSql + "\nArgs: " + data);
            }
        }
        return Pair.of(trimSql, data);
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
        this.sqlGenerator = new SqlGenerator(namedParamPrefix);
    }

    public boolean isReloadXqlOnGet() {
        return reloadXqlOnGet;
    }

    public void setReloadXqlOnGet(boolean reloadXqlOnGet) {
        this.reloadXqlOnGet = reloadXqlOnGet;
    }
}