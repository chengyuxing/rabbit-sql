package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.datasource.DataSourceUtil;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.exceptions.SqlRuntimeException;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.support.JdbcSupport;
import com.github.chengyuxing.sql.transaction.Tx;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * <p>如果配置了{@link SQLFileManager },则接口所有方法都可以通过 <b>&amp;文件夹名.文件名.sql</b> 名来获取sql文件内的sql,通过<b>&amp;</b>
 * 前缀符号来判断如果是sql名则获取sql否则当作sql直接执行</p>
 * 指定sql名执行：
 * <blockquote>
 * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; s = baki.query("&amp;data.query")) {
 *     s.map({@link DataRow}::toMap).forEach(System.out::println);
 *   }</pre>
 * </blockquote>
 *
 * @see JdbcSupport
 * @see Baki
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final DataSource dataSource;
    private DatabaseMetaData metaData;
    //---------optional properties------
    private SQLFileManager sqlFileManager;
    private boolean strictDynamicSqlArg = true;
    private boolean checkParameterType = true;

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
     * @param sqlFileManager sql文件解析管理器
     * @throws IOException        如果文件读取错误
     * @throws URISyntaxException 如果文件uri地址语法错误
     * @throws DuplicateException 如果同一个文件出现同名sql
     */
    public void setSqlFileManager(SQLFileManager sqlFileManager) throws IOException, URISyntaxException, DuplicateException {
        this.sqlFileManager = sqlFileManager;
        if (!sqlFileManager.isInitialized()) {
            sqlFileManager.init();
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
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public DataRow execute(String sql) {
        return execute(sql, Collections.emptyMap());
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param data      数据
     * @param strict    true：根据数据生成insert语句，不论表是否存在相应的字段，false：根据表字段筛选数据中存在的字段生成insert语句
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     * @see #fastInsert(String, Collection, boolean)
     * @see #fastInsert(String, Collection)
     */
    @Override
    public int insert(String tableName, Collection<? extends Map<String, ?>> data, boolean strict) {
        Iterator<? extends Map<String, ?>> iterator = data.iterator();
        if (iterator.hasNext()) {
            Map<String, ?> first = iterator.next();
            List<String> tableFields = strict ? new ArrayList<>() : getTableFields(tableName);
            String insertSql = SqlUtil.generatePreparedInsert(tableName, first, tableFields);
            return executeNonQuery(insertSql, data);
        }
        return -1;
    }

    /**
     * {@inheritDoc}
     * 根据数据生成insert语句，不论表是否存在相应的字段
     *
     * @param tableName 表名
     * @param data      数据
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     * @see #fastInsert(String, Collection, boolean)
     * @see #fastInsert(String, Collection)
     */
    @Override
    public int insert(String tableName, Collection<? extends Map<String, ?>> data) {
        return insert(tableName, data, true);
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param data      数据
     * @param strict    true：根据数据生成insert语句，不论表是否存在相应的字段，false：根据表字段筛选数据中存在的字段生成insert语句
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     * @see #fastInsert(String, Collection, boolean)
     * @see #fastInsert(String, Collection)
     */
    @Override
    public int insert(String tableName, Map<String, ?> data, boolean strict) {
        return insert(tableName, Collections.singletonList(data), strict);
    }

    /**
     * {@inheritDoc}
     * 根据数据生成insert语句，不论表是否存在相应的字段
     *
     * @param tableName 表名
     * @param data      数据
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     * @see #fastInsert(String, Collection, boolean)
     * @see #fastInsert(String, Collection)
     */
    @Override
    public int insert(String tableName, Map<String, ?> data) {
        return insert(tableName, Collections.singletonList(data));
    }

    /**
     * {@inheritDoc}（非预编译SQL）<br>
     * 注：不支持插入二进制对象
     *
     * @param tableName 表名
     * @param data      数据
     * @param strict    true：根据数据生成insert语句，不论表是否存在相应的字段，false：根据表字段筛选数据中存在的字段生成insert语句
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public int fastInsert(String tableName, Collection<? extends Map<String, ?>> data, boolean strict) {
        if (data.size() > 0) {
            Iterator<? extends Map<String, ?>> iterator = data.iterator();
            String[] sqls = new String[data.size()];
            List<String> tableFields = strict ? new ArrayList<>() : getTableFields(tableName);
            for (int i = 0; iterator.hasNext(); i++) {
                String insertSql = SqlUtil.generateInsert(tableName, iterator.next(), tableFields);
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
     * {@inheritDoc}（非预编译SQL）<br>
     * 注：不支持插入二进制对象<br>
     * 根据数据生成insert语句，不论表是否存在相应的字段
     *
     * @param tableName 表名
     * @param data      数据
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public int fastInsert(String tableName, Collection<? extends Map<String, ?>> data) {
        return fastInsert(tableName, data, true);
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param where     条件
     * @param arg       条件参数
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public int delete(String tableName, String where, Map<String, ?> arg) {
        String w = StringUtil.startsWithIgnoreCase(where.trim(), "where") ? where : "\nwhere " + where;
        return executeNonQuery("delete from " + tableName + w, arg);
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param where     条件
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public int delete(String tableName, String where) {
        return delete(tableName, where, Collections.emptyMap());
    }

    /**
     * {@inheritDoc}
     * e.g. {@code update(<table>, <Map>, "id = :id")}
     * 关于此方法的说明举例：
     * <blockquote>
     * <pre>
     *  参数： {id:14, name:'cyx', address:'kunming'}
     *  条件："id = :id"
     *  生成：update{@code <table>} set name = 'cyx', address = 'kunming'
     *       where id = 14
     *  </pre>
     * 解释：where中至少指定一个传名参数，数据中必须包含where条件中的所有传名参数
     * </blockquote>
     *
     * @param tableName 表名
     * @param data      数据：需要更新的数据和条件参数
     * @param where     条件：条件中需要有传名参数作为更新的条件依据
     * @return 受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误
     * @see Baki#fastUpdate(String, Collection, String)
     */
    @Override
    public int update(String tableName, Map<String, ?> data, String where) {
        Pair<String, List<String>> cnd = SqlUtil.generateSql(where, data, true);
        Map<String, Object> updateData = new HashMap<>(data);
        for (String key : cnd.getItem2()) {
            updateData.remove(key);
        }
        String update = SqlUtil.generatePreparedUpdate(tableName, updateData);
        String w = StringUtil.startsWithIgnoreCase(where.trim(), "where") ? where : "\nwhere " + where;
        return executeNonQuery(update + w, data);
    }

    /**
     * {@inheritDoc}（非预编译SQL）<br>
     * 注：不支持插入二进制对象<br>
     * 如果需要插入文件请使用：{@link Baki#update(String, Map, String)}（预编译SQL）<br>
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
     * @param args      参数：需要更新的数据和条件参数
     * @param where     条件：条件中需要有传名参数作为更新的条件依据
     * @return 受影响的行数
     * @throws SqlRuntimeException           执行批量操作时发生错误
     * @throws UnsupportedOperationException 数据库或驱动版本不支持批量操作
     * @throws IllegalArgumentException      数据条数少于一条
     */
    @Override
    public int fastUpdate(String tableName, Collection<? extends Map<String, ?>> args, String where) {
        if (args.size() > 0) {
            String[] sqls = new String[args.size()];
            Iterator<? extends Map<String, ?>> iterator = args.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                String update = SqlUtil.generateUpdate(tableName, iterator.next(), where);
                sqls[i] = update;
            }
            log.debug("preview sql: {}\nmore...", SqlUtil.highlightSql(sqls[0]));
            int count = Arrays.stream(batchExecute(sqls)).sum();
            log.debug("{} rows updated!", count);
            return count;
        }
        return -1;
    }

    /**
     * {@inheritDoc}(一个流对象占用一个连接对象，需要手动关闭流或使用try-with-resource包裹)
     *
     * @param sql 查询sql
     * @return 收集为流的结果集
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public Stream<DataRow> query(String sql) {
        return query(sql, Args.create());
    }

    /**
     * {@inheritDoc}(一个流对象占用一个连接对象，需要手动关闭流或使用try-with-resource包裹)
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 收集为流的结果集
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public Stream<DataRow> query(String sql, Map<String, ?> args) {
        return executeQueryStream(sql, args);
    }

    /**
     * {@inheritDoc}
     *
     * @param recordQuery 查询sql
     * @param page        当前页
     * @param size        分页大小
     * @param <T>         类型参数
     * @return 分页构建器
     */
    @Override
    public <T> IPageable<T> query(String recordQuery, int page, int size) {
        return new Pageable<>(this, recordQuery, page, size);
    }

    /**
     * {@inheritDoc}
     *
     * @param sql 查询sql
     * @return 空或一条
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public Optional<DataRow> fetch(String sql) {
        return fetch(sql, Args.create());
    }

    /**
     * {@inheritDoc}
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 空或一条
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public Optional<DataRow> fetch(String sql, Map<String, ?> args) {
        try (Stream<DataRow> s = query(sql, args)) {
            return s.findFirst();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param sql sql
     * @return 是否存在
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public boolean exists(String sql) {
        return fetch(sql).isPresent();
    }

    /**
     * {@inheritDoc}
     *
     * @param sql  sql
     * @param args 参数
     * @return 是否存在
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     */
    @Override
    public boolean exists(String sql, Map<String, ?> args) {
        return fetch(sql, args).isPresent();
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
     * @throws SqlRuntimeException 存储过程或函数执行过程中出现错误
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
     * 根据严格模式获取表字段
     *
     * @param tableName 表名
     * @return 表字段
     * @throws SqlRuntimeException 执行查询表字段出现异常
     */
    private List<String> getTableFields(String tableName) {
        return execute("select * from " + tableName + " where 1 = 2", sc -> {
            sc.executeQuery();
            ResultSet fieldsResultSet = sc.getResultSet();
            List<String> fields = Arrays.asList(JdbcUtil.createNames(fieldsResultSet, ""));
            JdbcUtil.closeResultSet(fieldsResultSet);
            log.debug("all fields of table: {} {}", tableName, fields);
            return fields;
        });
    }

    /**
     * {@inheritDoc}
     *
     * @return 数据源元信息
     * @throws ConnectionStatusException 如果数据库关闭或者获取连接对象失败
     */
    @Override
    public DatabaseMetaData getMetaData() {
        try {
            if (metaData == null) {
                metaData = getConnection().getMetaData();
            }
            return metaData;
        } catch (SQLException throwables) {
            throw new ConnectionStatusException("fail to get metadata: ", throwables);
        }
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
    protected String prepareSql(String sql, Map<String, ?> args) {
        String trimEndedSql = SqlUtil.trimEnd(sql);
        if (sql.startsWith("&")) {
            if (sqlFileManager != null) {
                trimEndedSql = SqlUtil.trimEnd(sqlFileManager.get(sql.substring(1), args, strictDynamicSqlArg));
            } else {
                throw new NullPointerException("can not find property 'sqlFileManager' or SQLFileManager object init failed!");
            }
        }
        if (sqlFileManager != null) {
            Map<String, String> constants = sqlFileManager.getConstants();
            if (!constants.isEmpty()) {
                for (String key : constants.keySet()) {
                    String constantName = "${" + key + "}";
                    if (trimEndedSql.contains(constantName)) {
                        // use args first, if not exists then constants.
                        if (!args.containsKey(constantName)) {
                            trimEndedSql = trimEndedSql.replace(constantName, constants.get(key));
                        }
                    }
                }
            }
        }
        return trimEndedSql;
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
        DataSourceUtil.releaseConnectionIfNecessary(connection, dataSource);
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
}