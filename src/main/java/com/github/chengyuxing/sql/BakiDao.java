package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.CollectionUtil;
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
import com.github.chengyuxing.sql.utils.SqlTranslator;
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
 * 取SQL通过 {@code &}符号前缀+sql键名：
 * <blockquote>
 * e.g. 配置类型:
 * <pre>
 *  files: {
 *       sys: 'pgsql/test.sql',
 *       mac: 'file:/Users/chengyuxing/Downloads/local.sql'
 *   }
 * </pre>
 * </blockquote>
 * 指定sql名执行：
 * <blockquote>
 * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; s = baki.query("&amp;sys.getUser")) {
 *     s.forEach(System.out::println);
 *   }</pre>
 * </blockquote>
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final DataSource dataSource;
    private DatabaseMetaData currentMetaData;
    private SqlTranslator sqlTranslator = new SqlTranslator(':');
    //---------optional properties------
    private PageHelperProvider globalPageHelperProvider;
    private SqlInterceptor sqlInterceptor;
    private XQLFileManager xqlFileManager;
    private char namedParamPrefix = ':';
    private boolean strictDynamicSqlArg = true;
    private boolean checkParameterType = true;
    private boolean debugFullSql = false;
    private boolean highlightSql = false;

    /**
     * 构造函数
     *
     * @param dataSource 数据源
     */
    public BakiDao(DataSource dataSource) {
        this.dataSource = dataSource;
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
        this.xqlFileManager.setNamedParamPrefix(namedParamPrefix);
        this.xqlFileManager.setHighlightSql(highlightSql);
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
        this.sqlTranslator = new SqlTranslator(namedParamPrefix);
        if (xqlFileManager != null) {
            xqlFileManager.setNamedParamPrefix(namedParamPrefix);
        }
    }

    /**
     * 插入
     *
     * @param tableName 表名
     * @param data      数据
     * @param uncheck   <ul>
     *                  <li>true: 根据数据原封不动完全插入，如果有不存在的字段则抛出异常</li>
     *                  <li>false: 根据数据库表字段名排除数据中不存在的key，安全的插入</li>
     *                  </ul>
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误
     * @see #fastInsert(String, Collection, boolean)
     */
    protected int insert(String tableName, Collection<? extends Map<String, ?>> data, boolean uncheck) {
        if (data.isEmpty()) {
            return 0;
        }
        Map<String, ?> first = data.iterator().next();
        List<String> tableFields = uncheck ? new ArrayList<>() : getTableFields(tableName);
        String sql = sqlTranslator.generateNamedParamInsert(tableName, first, tableFields);
        return executeNonQuery(sql, data);
    }

    /**
     * 插入（非预编译SQL）<br>
     * 注：不支持插入二进制对象
     *
     * @param tableName 表名
     * @param data      数据
     * @param uncheck   <ul>
     *                  <li>true: 根据数据原封不动完全插入，如果有不存在的字段则抛出异常</li>
     *                  <li>false: 根据数据库表字段名排除数据中不存在的key，安全的插入</li>
     *                  </ul>
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误
     */
    protected int fastInsert(String tableName, Collection<? extends Map<String, ?>> data, boolean uncheck) {
        if (data.size() > 0) {
            Iterator<? extends Map<String, ?>> iterator = data.iterator();
            List<String> sqls = new ArrayList<>(data.size());
            List<String> tableFields = uncheck ? new ArrayList<>() : getTableFields(tableName);
            while (iterator.hasNext()) {
                String insertSql = sqlTranslator.generateInsert(tableName, iterator.next(), tableFields);
                sqls.add(insertSql);
            }
            log.debug("preview sql: {}\nmore...", SqlUtil.buildPrintSql(sqls.get(0), highlightSql));
            int count = super.executeBatch(sqls).length;
            log.debug("{} rows inserted!", count);
            return count;
        }
        return 0;
    }

    /**
     * 更新<br>
     * e.g. {@code update(<table>, <List>, "id = :id")}<br>
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
     * @param tableName 表名
     * @param data      数据：需要更新的数据和条件参数
     * @param uncheck   <ul>
     *                  <li>true: 根据数据原封不动进行更新，如果有不存在的字段则抛出异常</li>
     *                  <li>false: 根据数据库表字段名排除数据中不存在的key，安全的更新</li>
     *                  </ul>
     * @param where     条件：条件中需要有传名参数作为更新的条件依据
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误
     */
    protected int update(String tableName, Collection<? extends Map<String, ?>> data, boolean uncheck, String where) {
        if (data.isEmpty()) {
            return 0;
        }
        String whereSql = getSql(where, Collections.emptyMap());
        // 这里是防止引用类型导致移除了必要的参数
        Map<String, ?> first = new HashMap<>(data.iterator().next());
        List<String> tableFields = uncheck ? new ArrayList<>() : getTableFields(tableName);
        // 获取where条件中的参数名
        List<String> whereFields = sqlTranslator.getPreparedSql(whereSql).getItem2();
        for (String key : whereFields) {
            // 如果where条件中参数名是小写，而第一行数据中是大写，则也需要删除那个数据，来保证生成正确的set更新数据块
            if (CollectionUtil.containsKeyIgnoreCase(first, key))
                first.remove(key);
        }
        String update = sqlTranslator.generateNamedParamUpdate(tableName, first, tableFields);
        String sql = update + "\nwhere " + whereSql;
        return executeNonQuery(sql, data);
    }

    /**
     * 更新（非预编译SQL）<br>
     * 注：不支持更新二进制对象<br>
     * e.g. {@code fastUpdate(<table>, <List<Map>>, "id = :id")}
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
     * @param tableName 表名
     * @param data      参数：需要更新的数据和条件参数
     * @param uncheck   <ul>
     *                  <li>true: 根据数据原封不动进行更新，如果有不存在的字段则抛出异常</li>
     *                  <li>false: 根据数据库表字段名排除数据中不存在的key，安全的更新</li>
     *                  </ul>
     * @param where     条件：条件中需要有传名参数作为更新的条件依据
     * @return 受影响的行数
     * @throws UncheckedSqlException         执行批量操作时发生错误
     * @throws UnsupportedOperationException 数据库或驱动版本不支持批量操作
     * @throws IllegalArgumentException      数据条数少于一条
     */
    protected int fastUpdate(String tableName, Collection<? extends Map<String, ?>> data, boolean uncheck, String where) {
        if (data.isEmpty()) {
            return 0;
        }
        String whereSql = getSql(where, Collections.emptyMap());
        List<String> sqls = new ArrayList<>(data.size());
        Map<String, Object> first = new HashMap<>(data.iterator().next());
        List<String> tableFields = uncheck ? new ArrayList<>() : getTableFields(tableName);
        // 获取where条件中的参数名
        List<String> whereFields = sqlTranslator.getPreparedSql(whereSql).getItem2();
        // 将where条件中的参数排除，因为where中的参数作为条件，而不是需要更新的值
        for (String key : whereFields) {
            if (CollectionUtil.containsKeyIgnoreCase(first, key))
                first.remove(key);
        }
        // 以第一条记录构建出确定的传名参数的预编译sql，后续再处理为非预编译sql
        String update = sqlTranslator.generateNamedParamUpdate(tableName, first, tableFields);
        String fullUpdatePrepared = update + "\nwhere " + whereSql;
        for (Map<String, ?> item : data) {
            // 完整的参数字典
            String updateNonPrepared = sqlTranslator.generateSql(fullUpdatePrepared, item, false).getItem1();
            sqls.add(updateNonPrepared);
        }
        log.debug("preview sql: {}\nmore...", SqlUtil.buildPrintSql(sqls.get(0), highlightSql));
        int count = Arrays.stream(super.executeBatch(sqls)).sum();
        log.debug("{} rows updated!", count);
        return count;
    }

    @Override
    public Executor of(String sql, String... more) {
        return new Executor(sql, more) {
            private List<String> allSql() {
                List<String> all = new ArrayList<>();
                all.add(sql);
                if (more.length > 0) {
                    all.addAll(Arrays.asList(more));
                }
                return all;
            }

            @Override
            public int[] executeBatch() {
                return BakiDao.super.executeBatch(allSql());
            }

            @Override
            public int executeBatch(Collection<Map<String, ?>> argsForBatch) {
                return executeNonQuery(sql, argsForBatch);
            }

            @Override
            public DataRow execute() {
                return BakiDao.super.execute(sql, args);
            }
        };
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
                String query = getSql(sql, args);
                try (Stream<DataRow> s = executeQueryStream(pageHelper.pagedSql(query), args)) {
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
                    return fastUpdate(tableName, data, !safe, where);
                }
                return update(tableName, data, !safe, where);
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
                    return fastInsert(tableName, data, !safe);
                }
                return insert(tableName, data, !safe);
            }
        };
    }

    @Override
    public DeleteExecutor delete(String tableName) {
        return new DeleteExecutor(tableName) {
            @Override
            public int execute(String where) {
                String whereSql = getSql(where, Collections.emptyMap());
                String w = StringUtil.startsWithIgnoreCase(whereSql.trim(), "where") ? whereSql : "\nwhere " + whereSql;
                return executeNonQuery("delete from " + tableName + w, Collections.singletonList(args));
            }
        };
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
            String query = getSql(recordQuery, args);
            if (count == null) {
                String cq = countQuery;
                if (cq == null) {
                    cq = sqlTranslator.generateCountQuery(query);
                }
                DataRow cnRow = execute(cq, args).getFirstAs();
                Object cn = cnRow.getFirst();
                if (cn instanceof Integer) {
                    count = (Integer) cn;
                } else {
                    count = Integer.parseInt(cn.toString());
                }
            }

            String dbName;
            try {
                dbName = metaData().getDatabaseProductName().toLowerCase();
            } catch (SQLException e) {
                throw new UncheckedSqlException("get database metadata error: ", e);
            }

            PageHelper pageHelper = null;

            if (pageHelperProvider != null) {
                pageHelper = pageHelperProvider.customPageHelper(metaData(), dbName, namedParamPrefix);
            }

            if (pageHelper == null) {
                pageHelper = defaultPager();
            }
            pageHelper.init(page, size, count);
            Map<String, Integer> pagedArgs = pageHelper.pagedArgs();
            if (pagedArgs == null) {
                pagedArgs = new HashMap<>();
            }
            args.putAll(rewriteArgsFunc == null ? pagedArgs : rewriteArgsFunc.apply(pagedArgs));
            String executeQuery = disablePageSql ? query : pageHelper.pagedSql(query);
            try (Stream<DataRow> s = executeQueryStream(executeQuery, args)) {
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
     * 根据数据库名字自动选择合适的默认分页帮助类
     *
     * @return 分页帮助类
     * @throws UnsupportedOperationException 如果没有自定分页，而默认分页不支持当前数据库
     * @throws ConnectionStatusException     如果连接对象异常
     */
    protected PageHelper defaultPager() {
        try {
            String dbName = metaData().getDatabaseProductName().toLowerCase();
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
        } catch (SQLException e) {
            throw new UncheckedSqlException("get database metadata error: ", e);
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
        String sql = getSql("select * from " + tableName + " where 1 = 2", Collections.emptyMap());
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
    protected String getSql(String sql, Map<String, ?> args) {
        String trimEndedSql = SqlUtil.trimEnd(sql);
        // 如果是sql名则从文件取sql
        if (sql.startsWith("&")) {
            if (xqlFileManager != null) {
                // 经过XQLFileManager获取的sql，已经去除了段落注释和行注释
                // 内部的自定义常量也替换完成，后续都没必要再来一次
                trimEndedSql = xqlFileManager.get(sql.substring(1), args, strictDynamicSqlArg);
            } else {
                throw new NullPointerException("can not find property 'xqlFileManager' or XQLFileManager object init failed!");
            }
        }
        if (trimEndedSql.contains("${") && xqlFileManager != null) {
            Map<String, String> constants = xqlFileManager.getConstants();
            for (String constantName : constants.keySet()) {
                // 如果用户参数中不存在，才去常量里找，用户参数优先级最高
                if (args == null || !args.containsKey(constantName)) {
                    trimEndedSql = StringUtil.format(trimEndedSql, constantName, constants.get(constantName));
                }
            }
        }
        if (sqlInterceptor != null) {
            String error = "reject to execute invalid sql.\nSQL: " + trimEndedSql + "\nArgs: " + args;
            boolean request;
            try {
                request = sqlInterceptor.prevHandle(trimEndedSql, args, metaData());
            } catch (Throwable e) {
                throw new IllegalSqlException(error, e);
            }
            if (!request) {
                throw new IllegalSqlException(error);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("SQL: {}", SqlUtil.buildPrintSql(trimEndedSql, highlightSql));
            log.debug("Args: {}", args);
            if (debugFullSql) {
                String fullSql = sqlTranslator().generateSql(trimEndedSql, args, false).getItem1();
                log.debug("Full SQL: {}", SqlUtil.buildPrintSql(fullSql, highlightSql));
            }
        }
        return trimEndedSql;
    }

    @Override
    protected SqlTranslator sqlTranslator() {
        return sqlTranslator;
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
     * 设置动态sql的参数是否为严格模式
     *
     * @param strictDynamicSqlArg 严格模式
     */
    public void setStrictDynamicSqlArg(boolean strictDynamicSqlArg) {
        this.strictDynamicSqlArg = strictDynamicSqlArg;
    }

    /**
     * 获取当前动态sql参数是否为严格模式
     *
     * @return 当前严格模式状态
     */
    public boolean isStrictDynamicSqlArg() {
        return strictDynamicSqlArg;
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

    /**
     * 设置是否打印拼接完整的SQL，否则只打印原始SQL与参数
     *
     * @param debugFullSql 是否调试模式输出拼接完整的sql
     */
    public void setDebugFullSql(boolean debugFullSql) {
        this.debugFullSql = debugFullSql;
    }

    /**
     * debug模式下终端标准输出sql语法是否高亮
     *
     * @return 是否高亮
     */
    public boolean isHighlightSql() {
        return highlightSql;
    }

    /**
     * 设置debug模式下终端标准输出sql语法是否高亮
     *
     * @param highlightSql 是否高亮
     */
    public void setHighlightSql(boolean highlightSql) {
        this.highlightSql = highlightSql;
        if (xqlFileManager != null) {
            xqlFileManager.setHighlightSql(highlightSql);
        }
    }
}