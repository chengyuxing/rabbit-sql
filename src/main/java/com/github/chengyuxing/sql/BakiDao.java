package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.datasource.DataSourceUtil;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.MysqlPageHelper;
import com.github.chengyuxing.sql.page.impl.OraclePageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.support.JdbcSupport;
import com.github.chengyuxing.sql.support.executor.InsertExecutor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.support.executor.UpdateExecutor;
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
 * <p>如果配置了{@link XQLFileManager },则接口所有方法都可以通过取地址符号来获取sql文件内的sql</p>
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
 *
 * @see JdbcSupport
 * @see Baki
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final DataSource dataSource;
    private DatabaseMetaData currentMetaData;
    private SqlTranslator sqlTranslator = new SqlTranslator(':');
    //---------optional properties------
    private Map<String, Class<? extends PageHelper>> pageHelpers = new HashMap<>();
    private XQLFileManager xqlFileManager;
    private char namedParamPrefix = ':';
    private boolean strictDynamicSqlArg = true;
    private boolean checkParameterType = true;
    private boolean debugFullSql = false;

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
     * 执行query语句，ddl或dml语句<br>
     * 返回数据为:<br>
     * 执行结果：{@code DataRow.get(0)} 或 {@code DataRow.get("result")}<br>
     * 执行类型：{@code DataRow.get(1)} 或 {@code DataRow.getString("type")}
     *
     * @param sql 原始sql
     * @return (结果 ， 类型)
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public DataRow execute(String sql) {
        return execute(sql, Collections.emptyMap());
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
            String[] sqls = new String[data.size()];
            List<String> tableFields = uncheck ? new ArrayList<>() : getTableFields(tableName);
            for (int i = 0; iterator.hasNext(); i++) {
                String insertSql = sqlTranslator.generateInsert(tableName, iterator.next(), tableFields);
                sqls[i] = insertSql;
            }
            log.debug("preview sql: {}\nmore...", SqlUtil.highlightSql(sqls[0]));
            int count = batchExecute(sqls).length;
            log.debug("{} rows inserted!", count);
            return count;
        }
        return -1;
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param where     条件
     * @param arg       条件参数
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public int delete(String tableName, String where, Map<String, ?> arg) {
        String w = StringUtil.startsWithIgnoreCase(where.trim(), "where") ? where : "\nwhere " + where;
        return executeNonQuery("delete from " + tableName + w, Collections.singletonList(arg));
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param where     条件
     * @return 受影响的行数
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public int delete(String tableName, String where) {
        return delete(tableName, where, Collections.emptyMap());
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
            public <T> IPageable<T> pageable(int page, int size) {
                IPageable<T> iPageable = new SimplePageable<>(sql, page, size);
                return iPageable.args(args);
            }

            @Override
            public DataRow findFirstRow() {
                return findFirst().orElseGet(DataRow::new);
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
        // 这里是防止引用类型导致移除了必要的参数
        Map<String, ?> first = new HashMap<>(data.iterator().next());
        List<String> tableFields = uncheck ? new ArrayList<>() : getTableFields(tableName);
        // 获取where条件中的参数名
        List<String> whereFields = sqlTranslator.getPreparedSql(where, Collections.emptyMap()).getItem2();
        for (String key : whereFields) {
            first.remove(key);
        }
        String update = sqlTranslator.generateNamedParamUpdate(tableName, first, tableFields);
        String sql = update + "\nwhere " + where;
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
        String[] sqls = new String[data.size()];
        Map<String, Object> first = new HashMap<>(data.iterator().next());
        List<String> tableFields = uncheck ? new ArrayList<>() : getTableFields(tableName);
        // 获取where条件中的参数名
        List<String> whereFields = sqlTranslator.generateSql(where, Collections.emptyMap(), true).getItem2();
        // 将where条件中的参数排除，因为where中的参数作为条件，而不是需要更新的值
        for (String key : whereFields) {
            first.remove(key);
        }
        // 以第一条记录构建出确定的传名参数的预编译sql，后续再处理为非预编译sql
        String update = sqlTranslator.generateNamedParamUpdate(tableName, first, tableFields);
        String fullUpdatePrepared = update + "\nwhere " + where;
        Iterator<? extends Map<String, ?>> iterator = data.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            // 完整的参数字典
            Map<String, ?> item = iterator.next();
            String updateNonPrepared = sqlTranslator.generateSql(fullUpdatePrepared, item, false).getItem1();
            sqls[i] = updateNonPrepared;
        }
        log.debug("preview sql: {}\nmore...", SqlUtil.highlightSql(sqls[0]));
        int count = Arrays.stream(batchExecute(sqls)).sum();
        log.debug("{} rows updated!", count);
        return count;
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

    /**
     * 简单的分页构建器实现
     *
     * @param <T> 结果类型参数
     */
    class SimplePageable<T> extends IPageable<T> {

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
        public PagedResource<T> collect(Function<DataRow, T> mapper) {
            String query = getSql(recordQuery, args);
            if (count == null) {
                String cq = countQuery;
                if (cq == null) {
                    cq = sqlTranslator.generateCountQuery(query);
                }
                count = execute(cq, args).<DataRow>getFirstAs().getInt(0);
            }
            PageHelper pageHelper = customPageHelper;
            if (pageHelper == null) {
                pageHelper = defaultPager();
            }
            pageHelper.init(page, size, count);
            args.putAll(rewriteArgsFunc == null ? pageHelper.pagedArgs() : rewriteArgsFunc.apply(pageHelper.pagedArgs()));
            String executeQuery = disablePageSql ? query : pageHelper.pagedSql(query);
            try (Stream<DataRow> s = executeQueryStream(executeQuery, args)) {
                List<T> list = s.map(mapper).collect(Collectors.toList());
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
     *         baki.call("{call test.func2(:c::refcursor)}",
     *             Args.create("c",Param.IN_OUT("result", OUTParamType.REF_CURSOR))
     *             ).get(0));
     * </pre>
     * </blockquote>
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return DataRow
     * @throws UncheckedSqlException 存储过程或函数执行过程中出现错误
     * @see #executeCallStatement(String, Map)
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
            if (!pageHelpers.isEmpty()) {
                if (pageHelpers.containsKey(dbName))
                    return pageHelpers.get(dbName).newInstance();
            }
            switch (dbName) {
                case "oracle":
                    return new OraclePageHelper();
                case "postgresql":
                case "sqlite":
                    return new PGPageHelper();
                case "mysql":
                    return new MysqlPageHelper();
                default:
                    throw new UnsupportedOperationException("pager of \"" + dbName + "\" default not implement currently, see method 'pageHelpers' or 'registerPageHelper'.");
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException("get database metadata error: ", e);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据严格模式获取表字段
     *
     * @param tableName 表名
     * @return 表字段
     * @throws UncheckedSqlException 执行查询表字段出现异常
     */
    private List<String> getTableFields(String tableName) {
        String sql = getSql("select * from " + tableName + " where 1 = 2", Collections.emptyMap());
        return execute(sql, sc -> {
            sc.executeQuery();
            ResultSet fieldsResultSet = sc.getResultSet();
            List<String> fields = Arrays.asList(JdbcUtil.createNames(fieldsResultSet, ""));
            JdbcUtil.closeResultSet(fieldsResultSet);
            log.debug("all fields of table: {} {}", tableName, fields);
            return fields;
        });
    }

    /**
     * 如果使用取地址符"&amp;sql文件名.sql名"则获取sql文件中已缓存的sql
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
                return trimEndedSql;
            } else {
                throw new NullPointerException("can not find property 'xqlFileManager' or XQLFileManager object init failed!");
            }
        }
        // 如果是sql字符串，没有字符串模版占位符，也没必要再去查找
        if (!trimEndedSql.contains("${")) {
            return trimEndedSql;
        }
        boolean hasArgs = args != null && !args.isEmpty();
        if (xqlFileManager != null) {
            Map<String, String> constants = xqlFileManager.getConstants();
            if (!constants.isEmpty()) {
                for (String key : constants.keySet()) {
                    String constantName = "${" + key + "}";
                    if (trimEndedSql.contains(constantName)) {
                        // use args first, if not exists then constants.
                        if (!hasArgs || !args.containsKey(constantName)) {
                            trimEndedSql = trimEndedSql.replace(constantName, constants.get(key));
                        }
                    }
                }
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
    protected boolean debugFullSql() {
        return debugFullSql;
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
        DataSourceUtil.releaseConnectionIfNecessary(connection, dataSource);
    }

    /**
     * 设置自定义的分页帮助工具类实现
     *
     * @param pageHelpers 分页帮助类集合 [数据库名字: 分页帮助工具类类名]
     * @see DatabaseMetaData#getDatabaseProductName()
     */
    @SuppressWarnings("unchecked")
    public void configPageHelpers(Map<String, String> pageHelpers) {
        Map<String, Class<? extends PageHelper>> map = new HashMap<>();
        try {
            for (Map.Entry<String, String> e : pageHelpers.entrySet()) {
                map.put(e.getKey(), (Class<? extends PageHelper>) Class.forName(e.getValue()));
            }
            this.pageHelpers = map;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 设置自定义的分页帮助工具类实现
     *
     * @param pageHelpers 分页帮助类集合 [数据库名字: 分页帮助工具类]
     */
    public void setPageHelpers(Map<String, Class<? extends PageHelper>> pageHelpers) {
        this.pageHelpers = pageHelpers;
    }

    /**
     * 注册分页帮助工具类
     *
     * @param databaseName 数据库名字，来自于：{@link DatabaseMetaData#getDatabaseProductName()}
     * @param pageHelper   分页帮助工具类
     */
    public void registerPageHelper(String databaseName, Class<? extends PageHelper> pageHelper) {
        this.pageHelpers.put(databaseName.toLowerCase(), pageHelper);
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
}