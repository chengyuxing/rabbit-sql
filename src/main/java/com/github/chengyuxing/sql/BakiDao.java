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
 * <h2>Baki接口默认实现</h2>
 * <p>如果配置了{@link XQLFileManager }，则接口所有方法都可以通过取地址符号来获取sql文件内的sql。</p>
 * 指定sql名执行：
 * <blockquote>
 * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; s = baki.query("&amp;sys.getUser").stream()) {
 *     s.forEach(System.out::println);
 *   }</pre>
 * </blockquote>
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private DataSource dataSource;
    private DatabaseMetaData currentMetaData;
    private String databaseId;
    private SqlGenerator sqlGenerator;
    //---------optional properties------
    /**
     * 全局自定义的分页帮助提供程序。
     */
    private PageHelperProvider globalPageHelperProvider;
    /**
     * sql预处理拦截器。
     */
    private SqlInterceptor sqlInterceptor;
    /**
     * 预编译sql参数值处理器。
     */
    private StatementValueHandler statementValueHandler;
    /**
     * sql文件解析管理器。
     */
    private XQLFileManager xqlFileManager;
    /**
     * 批量执行大小。
     */
    private int batchSize = 1000;
    /**
     * 命名参数前缀。
     */
    private char namedParamPrefix = ':';
    /**
     * 在获取sql时如果xql文件有变更则重新加载xql文件管理器。
     */
    private boolean reloadXqlOnGet = false;

    /**
     * 构造一个新的BakiDao实例。
     *
     * @param dataSource 数据源
     */
    public BakiDao(DataSource dataSource) {
        this.dataSource = dataSource;
        this.sqlGenerator = new SqlGenerator(namedParamPrefix);
        this.statementValueHandler = (ps, index, value, metaData) -> JdbcUtil.setStatementValue(ps, index, value);
        this.sqlInterceptor = (sql, args, metaData) -> true;
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

    /**
     * {@inheritDoc}<br>
     * 执行完成后自动关闭连接对象，不需要手动关闭。
     *
     * @param func 函数体
     * @param <T>  类型参数
     * @return 执行结果
     * @throws ConnectionStatusException 如果数据库错误或连接对象已关闭
     */
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

    /**
     * {@inheritDoc}
     *
     * @return 获取当前数据库元数据对象
     */
    public DatabaseMetaData metaData() {
        if (currentMetaData != null) {
            return currentMetaData;
        }
        return using(c -> {
            try {
                currentMetaData = c.getMetaData();
                return currentMetaData;
            } catch (SQLException e) {
                throw new UncheckedSqlException("get metadata error.", e);
            }
        });
    }

    /**
     * 简单的分页构建器实现。
     */
    class SimplePageable extends IPageable {

        /**
         * 简单分页构建器构造函数。
         *
         * @param recordQuery 查询sql
         * @param page        当前页
         * @param size        页大小
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
                //noinspection unchecked
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
                pageHelper = pageHelperProvider.customPageHelper(metaData(), databaseId(), namedParamPrefix);
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
     * 当前数据库的名称。
     *
     * @return 数据库的名称
     */
    protected String databaseId() {
        if (databaseId == null) {
            DatabaseMetaData metaData = metaData();
            try {
                databaseId = metaData.getDatabaseProductName().toLowerCase();
            } catch (SQLException e) {
                throw new UncheckedSqlException("get database id error.", e);
            }
        }
        return databaseId;
    }

    /**
     * 根据数据库名字自动选择合适的默认分页帮助类。
     *
     * @return 分页帮助类
     * @throws UnsupportedOperationException 如果没有自定分页，而默认分页不支持当前数据库
     * @throws ConnectionStatusException     如果连接对象异常
     */
    protected PageHelper defaultPager() {
        String dbName = databaseId();
        if (Objects.nonNull(globalPageHelperProvider)) {
            PageHelper pageHelper = globalPageHelperProvider.customPageHelper(metaData(), dbName, namedParamPrefix);
            if (Objects.nonNull(pageHelper)) {
                return pageHelper;
            }
        }
        switch (dbName) {
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
                throw new UnsupportedOperationException("pager of \"" + dbName + "\" default not implement currently, see method 'setPageHelperProvider'.");
        }
    }

    /**
     * 根据严格模式获取表字段。
     *
     * @param tableName 表名
     * @return 表字段
     * @throws UncheckedSqlException 执行查询表字段出现异常
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
     * 如果使用取地址符 {@code &sql文件别名.sql名} 则获取sql文件中已缓存的sql。
     *
     * @param sql  sql或sql名
     * @param args 参数
     * @return sql
     * @throws NullPointerException     如果没有设置sql文件解析器或初始化，使用{@code &}引用用外部sql文件片段
     * @throws IllegalArgumentException 如果严格模式下动态sql参数为null或空
     * @throws IllegalSqlException      sql拦截器拒绝执行
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
                trimSql = result.getItem1();
                // #for表达式的临时变量存储在 _for 变量中
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
        boolean request = sqlInterceptor.preHandle(trimSql, data, metaData());
        if (!request) {
            throw new IllegalSqlException("permission denied, reject to execute invalid sql.\nSQL: " + trimSql + "\nArgs: " + data);
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

    protected void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
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
        statementValueHandler.preHandle(ps, index, value, metaData());
    }

    public void setGlobalPageHelperProvider(PageHelperProvider globalPageHelperProvider) {
        if (Objects.nonNull(globalPageHelperProvider))
            this.globalPageHelperProvider = globalPageHelperProvider;
    }

    public void setSqlInterceptor(SqlInterceptor sqlInterceptor) {
        if (Objects.nonNull(sqlInterceptor))
            this.sqlInterceptor = sqlInterceptor;
    }

    public void setStatementValueHandler(StatementValueHandler statementValueHandler) {
        if (Objects.nonNull(statementValueHandler))
            this.statementValueHandler = statementValueHandler;
    }

    public void setXqlFileManager(XQLFileManager xqlFileManager) {
        if (Objects.nonNull(xqlFileManager)) {
            this.xqlFileManager = xqlFileManager;
            this.xqlFileManager.setDatabaseId(databaseId());
            if (!xqlFileManager.isInitialized()) {
                xqlFileManager.init();
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