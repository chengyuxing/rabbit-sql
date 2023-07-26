package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.datasource.DataSourceUtil;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
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
import com.github.chengyuxing.sql.support.executor.*;
import com.github.chengyuxing.sql.transaction.Tx;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <h2>数据库DAO对象实现</h2>
 * <p>如果配置了{@link XQLFileManager }，则接口所有方法都可以通过取地址符号来获取sql文件内的sql</p>
 * 指定sql名执行：
 * <blockquote>
 * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; s = baki.query("&amp;sys.getUser").stream()) {
 *     s.forEach(System.out::println);
 *   }</pre>
 * </blockquote>
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final DataSource dataSource;
    private DatabaseMetaData currentMetaData;
    private String databaseId;
    private SqlGenerator sqlGenerator;
    //---------optional properties------
    private PageHelperProvider globalPageHelperProvider;
    private SqlInterceptor sqlInterceptor;
    private XQLFileManager xqlFileManager;
    private char namedParamPrefix = ':';
    private boolean checkParameterType = false;

    /**
     * 构造函数
     *
     * @param dataSource 数据源
     */
    public BakiDao(DataSource dataSource) {
        this.dataSource = dataSource;
        this.sqlGenerator = new SqlGenerator(namedParamPrefix);
    }

    /**
     * 实例化一个BakiDao对象
     *
     * @param dataSource 数据源
     * @return BakiDao实例
     */
    public static BakiDao of(DataSource dataSource) {
        return new BakiDao(dataSource);
    }

    /**
     * 指定sql文件解析管理器
     *
     * @param xqlFileManager sql文件解析管理器
     * @throws java.io.UncheckedIOException 如果文件读取错误
     * @throws RuntimeException             如果文件uri地址语法错误
     * @throws DuplicateException           如果同一个文件出现同名sql
     */
    public void setXqlFileManager(XQLFileManager xqlFileManager) {
        this.xqlFileManager = xqlFileManager;
        if (!xqlFileManager.isInitialized()) {
            xqlFileManager.init();
        }
    }

    /**
     * 获取当前sql文件解析管理器
     *
     * @return sql文件解析管理器
     */
    public XQLFileManager getXqlFileManager() {
        return xqlFileManager;
    }

    /**
     * 获取命名参数前缀
     *
     * @return 命名参数前缀
     */
    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }

    /**
     * 设置命名参数前缀
     *
     * @param namedParamPrefix 命名参数前缀
     */
    public void setNamedParamPrefix(char namedParamPrefix) {
        this.namedParamPrefix = namedParamPrefix;
        this.sqlGenerator = new SqlGenerator(namedParamPrefix);
    }

    /**
     * 插入
     *
     * @param tableName 表名
     * @param data      数据
     * @param safe      <ul>
     *                  <li>true: 根据数据库表字段名排除数据中不存在的key，安全的插入</li>
     *                  <li>false: 根据数据原封不动完全插入，如果有不存在的字段则抛出异常</li>
     *                  </ul>
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误
     * @see #fastInsert(String, Collection, boolean, boolean)
     */
    protected int insert(String tableName, Collection<? extends Map<String, ?>> data, boolean safe, boolean ignoreNull) {
        if (data.isEmpty()) {
            return 0;
        }
        List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
        int i = 0;
        for (Map<String, ?> item : data) {
            String sql = sqlGenerator.generateNamedParamInsert(tableName, item, tableFields, ignoreNull);
            i += executeNonQuery(sql, item);
        }
        return i;
    }

    /**
     * 插入（非预编译SQL）<br>
     * 注：不支持插入二进制对象
     *
     * @param tableName  表名
     * @param data       数据
     * @param safe       <ul>
     *                   <li>true: 根据数据库表字段名排除数据中不存在的key，安全的插入</li>
     *                   <li>false: 根据数据原封不动完全插入，如果有不存在的字段则抛出异常</li>
     *                   </ul>
     * @param ignoreNull 忽略null值
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误
     */
    protected int fastInsert(String tableName, Collection<? extends Map<String, ?>> data, boolean safe, boolean ignoreNull) {
        if (data.size() > 0) {
            List<String> sqls = new ArrayList<>(data.size());
            List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
            for (Map<String, ?> item : data) {
                String sql = sqlGenerator.generateInsert(tableName, item, tableFields, ignoreNull);
                sqls.add(sql);
            }
            log.debug("preview sql: {}\nmore...", SqlUtil.buildConsoleSql(sqls.get(0)));
            int count = super.executeBatch(sqls).length;
            log.debug("{} rows inserted!", count);
            return count;
        }
        return 0;
    }

    /**
     * 更新<br>
     * e.g. {@code update(<table>, "id = :id", <List>)}<br>
     * 关于此方法的说明举例：
     * <blockquote>
     * 根据第一条数据生成预编译SQL
     * <pre>
     *  参数： {id:14, name:'cyx', address:'kunming'},{...}...
     *  条件："id = :id"
     *  生成：update{@code <table>} set name = :name, address = :address
     *       where id = :id
     *  </pre>
     * 解释：where中至少指定一个传名参数，数据中必须包含where条件中的所有传名参数
     * </blockquote>
     *
     * @param tableName  表名
     * @param where      条件：条件中需要有传名参数作为更新的条件依据
     * @param data       数据：需要更新的数据和条件参数
     * @param safe       <ul>
     *                   <li>true: 根据数据库表字段名排除数据中不存在的key，安全的更新</li>
     *                   <li>false: 根据数据原封不动进行更新，如果有不存在的字段则抛出异常</li>
     *                   </ul>
     * @param ignoreNull 忽略null值
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误
     */
    protected int update(String tableName, String where, Collection<? extends Map<String, ?>> data, boolean safe, boolean ignoreNull) {
        if (data.isEmpty()) {
            return 0;
        }
        String whereStatement = parseSql(where, Collections.emptyMap()).getItem1();
        List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
        int i = 0;
        for (Map<String, ?> item : data) {
            String updateStatement = sqlGenerator.generateNamedParamUpdate(tableName,
                    whereStatement,
                    item,
                    tableFields,
                    ignoreNull);
            i += executeNonQuery(updateStatement, item);
        }
        return i;
    }

    /**
     * 更新（非预编译SQL）<br>
     * 注：不支持更新二进制对象<br>
     * e.g. {@code fastUpdate(<table>, "id = :id", <List<Map>>)}
     * 关于此方法的说明举例：
     * <blockquote>
     * <pre>
     *  参数： [{id:14, name:'cyx', address:'kunming'},...]
     *  条件："id = :id"
     *  生成：update{@code <table>} set name = 'cyx', address = 'kunming'
     *       where id = 14, update...
     *  </pre>
     * 解释：where中至少指定一个传名参数，数据中必须包含where条件中的所有传名参数
     * </blockquote>
     *
     * @param tableName  表名
     * @param where      条件：条件中需要有传名参数作为更新的条件依据
     * @param data       参数：需要更新的数据和条件参数
     * @param safe       <ul>
     *                   <li>true: 根据数据库表字段名排除数据中不存在的key，安全的更新</li>
     *                   <li>false: 根据数据原封不动进行更新，如果有不存在的字段则抛出异常</li>
     *                   </ul>
     * @param ignoreNull 忽略null值
     * @return 受影响的行数
     * @throws UncheckedSqlException         执行批量操作时发生错误
     * @throws UnsupportedOperationException 数据库或驱动版本不支持批量操作
     * @throws IllegalArgumentException      数据条数少于一条
     */
    protected int fastUpdate(String tableName, String where, Collection<? extends Map<String, ?>> data, boolean safe, boolean ignoreNull) {
        if (data.isEmpty()) {
            return 0;
        }
        String whereStatement = parseSql(where, Collections.emptyMap()).getItem1();
        List<String> tableFields = safe ? getTableFields(tableName) : new ArrayList<>();
        List<String> sqls = new ArrayList<>(data.size());
        for (Map<String, ?> item : data) {
            String updateStatement = sqlGenerator.generateUpdate(tableName,
                    whereStatement,
                    item,
                    tableFields,
                    ignoreNull);
            sqls.add(updateStatement);
        }
        log.debug("preview sql: {}\nmore...", SqlUtil.buildConsoleSql(sqls.get(0)));
        int count = Arrays.stream(super.executeBatch(sqls)).sum();
        log.debug("{} rows updated!", count);
        return count;
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
            public IPageable pageable(int page, int size) {
                IPageable iPageable = new SimplePageable(sql, page, size);
                return iPageable.args(args);
            }

            @Override
            public DataRow findFirstRow() {
                return findFirst().orElseGet(DataRow::new);
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
                if (pagedArgs != null) {
                    args.putAll(pagedArgs);
                }
                Pair<String, Map<String, Object>> result = parseSql(sql, args);
                try (Stream<DataRow> s = executeQueryStream(pageHelper.pagedSql(result.getItem1()), result.getItem2())) {
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
    public UpdateExecutor update(String tableName, String where) {
        return new UpdateExecutor(tableName, where) {
            @Override
            public int save(Map<String, ?> data) {
                return save(Collections.singletonList(data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                if (fast) {
                    return fastUpdate(tableName, where, data, safe, ignoreNull);
                }
                return update(tableName, where, data, safe, ignoreNull);
            }
        };
    }

    @Override
    public InsertExecutor insert(String tableName) {
        return new InsertExecutor(tableName) {
            @Override
            public int save(Map<String, ?> data) {
                return save(Collections.singletonList(data));
            }

            @Override
            public int save(Collection<? extends Map<String, ?>> data) {
                if (fast) {
                    return fastInsert(tableName, data, safe, ignoreNull);
                }
                return insert(tableName, data, safe, ignoreNull);
            }
        };
    }

    @Override
    public DeleteExecutor delete(String tableName) {
        return new DeleteExecutor(tableName) {
            @Override
            public int execute(String where) {
                String whereSql = parseSql(where, Collections.emptyMap()).getItem1();
                String w = StringUtil.startsWithIgnoreCase(whereSql.trim(), "where") ? whereSql : "\nwhere " + whereSql;
                return executeNonQuery("delete from " + tableName + w, args);
            }
        };
    }

    @Override
    public DataRow execute(String sql) {
        return execute(sql, Collections.emptyMap());
    }

    @Override
    public int[] executeBatch(String namedSql, Collection<Map<String, ?>> data) {
        List<String> list = new ArrayList<>();
        for (Map<String, ?> item : data) {
            String sql = sqlGenerator.generateSql(namedSql, item);
            list.add(sql);
        }
        return executeBatch(list);
    }

    /**
     * 设置全局自定义的分页帮助提供程序
     *
     * @param globalPageHelperProvider 全局分页帮助提供程序
     */
    public void setGlobalPageHelperProvider(PageHelperProvider globalPageHelperProvider) {
        this.globalPageHelperProvider = globalPageHelperProvider;
    }

    /**
     * 设置sql预处理拦截器
     *
     * @param sqlInterceptor sql预处理拦截器
     */
    public void setSqlInterceptor(SqlInterceptor sqlInterceptor) {
        this.sqlInterceptor = sqlInterceptor;
    }

    /**
     * 简单的分页构建器实现
     */
    class SimplePageable extends IPageable {

        /**
         * 简单分页构建器构造函数
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
                DataRow cnRow = execute(cq, data).getFirstAs();
                Object cn = cnRow.getFirst();
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
            Map<String, Integer> pagedArgs = pageHelper.pagedArgs();
            if (pagedArgs == null) {
                pagedArgs = new HashMap<>();
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
     * {@inheritDoc}<br>
     * e.g. PostgreSQL执行获取一个游标类型的结果：
     * <blockquote>
     * <pre>
     *      {@link List}&lt;{@link DataRow}&gt; rows = {@link Tx}.using(() -&gt;
     *         baki.call("{call test.func(:c::refcursor)}",
     *             Args.create("c",Param.IN_OUT("result", OUTParamType.REF_CURSOR))
     *             ).get(0));
     * </pre>
     * </blockquote>
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return DataRow
     * @throws UncheckedSqlException 存储过程或函数执行过程中出现错误
     */
    @Override
    public DataRow call(String name, Map<String, Param> args) {
        return executeCallStatement(name, args);
    }

    /**
     * {@inheritDoc}<br>
     * 执行完成后自动关闭连接对象，不需要手动关闭
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
     * @return 获取当前数据库元数据对象（连接为关闭状态）
     */
    @Override
    public DatabaseMetaData metaData() {
        if (currentMetaData != null) {
            return currentMetaData;
        }
        return using(c -> {
            try {
                currentMetaData = c.getMetaData();
                return currentMetaData;
            } catch (SQLException e) {
                throw new UncheckedSqlException("get metadata error: ", e);
            }
        });
    }

    /**
     * 当前数据库的名称
     *
     * @return 数据库的名称
     */
    protected String databaseId() {
        if (databaseId == null) {
            DatabaseMetaData metaData = metaData();
            try {
                databaseId = metaData.getDatabaseProductName().toLowerCase();
            } catch (SQLException e) {
                throw new UncheckedSqlException("get database id error: ", e);
            }
        }
        return databaseId;
    }

    /**
     * 根据数据库名字自动选择合适的默认分页帮助类
     *
     * @return 分页帮助类
     * @throws UnsupportedOperationException 如果没有自定分页，而默认分页不支持当前数据库
     * @throws ConnectionStatusException     如果连接对象异常
     */
    protected PageHelper defaultPager() {
        String dbName = databaseId();
        if (globalPageHelperProvider != null) {
            PageHelper pageHelper = globalPageHelperProvider.customPageHelper(metaData(), dbName, namedParamPrefix);
            if (pageHelper != null) {
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
     * 根据严格模式获取表字段
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
            log.debug("all fields of table: {} {}", tableName, fields);
            return fields;
        });
    }

    /**
     * 如果使用取地址符 {@code &sql文件别名.sql名} 则获取sql文件中已缓存的sql
     *
     * @param sql  sql或sql名
     * @param args 参数
     * @return sql
     * @throws NullPointerException     如果没有设置sql文件解析器或初始化，使用{@code &}引用用外部sql文件片段
     * @throws IllegalArgumentException 如果严格模式下动态sql参数为null或空
     */
    @Override
    protected Pair<String, Map<String, Object>> parseSql(String sql, Map<String, ?> args) {
        Map<String, Object> data = new HashMap<>();
        if (args != null) {
            data.putAll(args);
        }
        data.putIfAbsent("_databaseId", databaseId());

        String trimSql = SqlUtil.trimEnd(sql.trim());
        if (trimSql.startsWith("&")) {
            if (xqlFileManager != null) {
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
            if (xqlFileManager != null) {
                trimSql = SqlUtil.formatSql(trimSql, xqlFileManager.getConstants());
            }
        }
        if (sqlInterceptor != null) {
            String error = "reject to execute invalid sql.\nSQL: " + trimSql + "\nArgs: " + data;
            boolean request;
            try {
                request = sqlInterceptor.prevHandle(trimSql, data, metaData());
            } catch (Throwable e) {
                throw new IllegalSqlException(error, e);
            }
            if (!request) {
                throw new IllegalSqlException(error);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("SQL: {}", SqlUtil.buildConsoleSql(trimSql));
            log.debug("Args: {}", data);
        }
        return Pair.of(trimSql, data);
    }

    @Override
    protected SqlGenerator sqlGenerator() {
        return sqlGenerator;
    }

    @Override
    protected boolean checkParameterType() {
        return checkParameterType;
    }

    @Override
    protected DataSource getDataSource() {
        return dataSource;
    }

    /**
     * {@inheritDoc}
     *
     * @return 连接对象
     * @throws ConnectionStatusException 如果连接对象异常
     */
    @Override
    protected Connection getConnection() {
        try {
            return DataSourceUtil.getConnection(dataSource);
        } catch (SQLException e) {
            throw new ConnectionStatusException("fetch connection failed:", e);
        }
    }

    @Override
    protected void releaseConnection(Connection connection, DataSource dataSource) {
        DataSourceUtil.releaseConnection(connection, dataSource);
    }

    /**
     * 是否检查预编译sql对应的参数类型
     *
     * @return 是否检查
     */
    public boolean isCheckParameterType() {
        return checkParameterType;
    }

    /**
     * 设置是否检查预编译sql对应的参数类型
     *
     * @param checkParameterType 是否检查标志
     */
    public void setCheckParameterType(boolean checkParameterType) {
        this.checkParameterType = checkParameterType;
    }
}