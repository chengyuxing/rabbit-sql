package rabbit.sql.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.common.types.DataRow;
import rabbit.sql.Baki;
import rabbit.sql.datasource.DataSourceUtil;
import rabbit.sql.page.IPageable;
import rabbit.sql.support.ICondition;
import rabbit.sql.support.JdbcSupport;
import rabbit.sql.types.DataFrame;
import rabbit.sql.types.Param;
import rabbit.sql.utils.JdbcUtil;
import rabbit.sql.utils.SqlUtil;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static rabbit.sql.utils.SqlUtil.dynamicSql;

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
 * @see rabbit.sql.support.JdbcSupport
 * @see Baki
 */
public class BakiDao extends JdbcSupport implements Baki {
    private final static Logger log = LoggerFactory.getLogger(BakiDao.class);
    private final DataSource dataSource;
    private DatabaseMetaData metaData;
    //---------optional properties------
    private SQLFileManager sqlFileManager;
    private boolean strictDynamicSqlArg = true;

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
     */
    public void setSqlFileManager(SQLFileManager sqlFileManager) {
        this.sqlFileManager = sqlFileManager;
        try {
            if (!sqlFileManager.isInitialized()) {
                sqlFileManager.init();
            }
        } catch (IOException e) {
            log.error("sql file is not exists:{}", e.getMessage());
        } catch (URISyntaxException e) {
            log.error(e.getMessage());
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
     */
    @Override
    public DataRow execute(String sql) {
        return execute(sql, Args.create());
    }

    /**
     * 执行query语句，ddl或dml语句<br>
     * 返回数据为:<br>
     * 执行结果：{@code DataRow.get(0)} 或 {@code DataRow.get("result")}<br>
     * 执行类型：{@code DataRow.get(1)} 或 {@code DataRow.getString("type")}
     *
     * @param sql  原始sql
     * @param args 参数
     * @return (结果 ， 类型)
     */
    @Override
    public DataRow execute(String sql, Map<String, Object> args) {
        return executeAny(sql, args);
    }

    /**
     * {@inheritDoc}
     *
     * @param dataFrame 数据对象
     * @return 受影响的行数
     * @see #fastInsert(DataFrame)
     */
    @Override
    public int insert(DataFrame dataFrame) {
        Collection<Map<String, Object>> data = dataFrame.getRows();
        Iterator<Map<String, Object>> iterator = data.iterator();
        if (iterator.hasNext()) {
            Map<String, Object> first = iterator.next();
            List<String> tableFields = dataFrame.isStrict() ? new ArrayList<>() : getTableFields(dataFrame);
            String insertSql = SqlUtil.generatePreparedInsert(dataFrame.getTableName(), first, dataFrame.getIgnore(), tableFields);
            return executeNonQuery(insertSql, data);
        }
        return -1;
    }

    /**
     * {@inheritDoc}（对数据类型有一定的限制）<br>
     * 注：不支持插入二进制对象
     *
     * @param dataFrame 数据对象
     * @return 受影响的行数
     */
    @Override
    public int fastInsert(DataFrame dataFrame) {
        Collection<Map<String, Object>> data = dataFrame.getRows();
        if (data.size() > 0) {
            Iterator<Map<String, Object>> iterator = data.iterator();
            String[] sqls = new String[data.size()];
            List<String> tableFields = null;
            for (int i = 0; iterator.hasNext(); i++) {
                if (tableFields == null) {
                    tableFields = dataFrame.isStrict() ? new ArrayList<>() : getTableFields(dataFrame);
                }
                String insertSql = SqlUtil.generateInsert(dataFrame.getTableName(), iterator.next(), dataFrame.getIgnore(), tableFields);
                sqls[i] = insertSql;
            }
            log.debug("preview sql: {}\nmore...", sqls[0]);
            int count = executeBatch(sqls).length;
            log.debug("{} rows inserted!", count);
            return count;
        }
        return -1;
    }

    /**
     * 根据严格模式获取表字段
     *
     * @param dataFrame 数据对象
     * @return 表字段
     */
    private List<String> getTableFields(DataFrame dataFrame) {
        log.debug("prepare for non-strict insert...");
        return execute(dataFrame.getTableFieldsSql(), sc -> {
            sc.executeQuery();
            ResultSet fieldsResultSet = sc.getResultSet();
            List<String> fields = Arrays.asList(JdbcUtil.createNames(fieldsResultSet, ""));
            JdbcUtil.closeResultSet(fieldsResultSet);
            log.debug("all fields of table: {} {}", dataFrame.getTableName(), fields);
            return fields;
        });
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param condition 条件配置
     * @return 受影响的行数
     */
    @Override
    public int delete(String tableName, ICondition condition) {
        return executeNonQuery("delete from " + tableName + " " + condition.getSql(), Collections.singletonList(condition.getArgs()));
    }

    /**
     * {@inheritDoc}
     *
     * @param tableName 表名
     * @param data      数据
     * @param condition 条件
     * @return 受影响的行数
     * @see #fastUpdate(String, Collection, String)
     */
    @Override
    public int update(String tableName, Map<String, Object> data, ICondition condition) {
        String update = SqlUtil.generatePreparedUpdate(tableName, data);
        data.putAll(condition.getArgs());
        return executeNonQuery(update + condition.getSql(), Collections.singletonList(data));
    }

    /**
     * {@inheritDoc}（对数据类型有一定的限制）<br>
     * 注：不支持插入二进制对象<br>
     * e.g. {@code fastUpdate(<table>, <List<Map>>, "id = :id")}
     * 关于此方法的说明举例：
     * <blockquote>
     * <pre>
     *  参数： [{id:14, name:'cyx', address:'kunming'},...]
     *  条件："id = :id"
     *  生存：update{@code <table>} set name = 'cyx', address = 'kunming'
     *       where id = 14
     *  </pre>
     * 解释：where中至少指定一个传名参数，数据中必须包含where条件中的所以传名参数
     * </blockquote>
     *
     * @param tableName 表名
     * @param args      参数：需要更新的数据和条件参数
     * @param where     条件：条件中需要有传名参数作为更新的条件依据
     * @return 受影响的行数
     */
    @Override
    public int fastUpdate(String tableName, Collection<Map<String, Object>> args, String where) {
        if (args.size() > 0) {
            String[] sqls = new String[args.size()];
            Iterator<Map<String, Object>> iterator = args.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                String update = SqlUtil.generateUpdate(tableName, iterator.next(), where);
                sqls[i] = update;
            }
            log.debug("preview sql: {}\nmore...", sqls[0]);
            int count = Arrays.stream(executeBatch(sqls)).sum();
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
     */
    @Override
    public Stream<DataRow> query(String sql, Map<String, Object> args) {
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
     */
    @Override
    public Optional<DataRow> fetch(String sql, Map<String, Object> args) {
        try (Stream<DataRow> s = query(sql, args)) {
            return s.findFirst();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param sql sql
     * @return 是否存在
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
     */
    @Override
    public boolean exists(String sql, Map<String, Object> args) {
        return fetch(sql, args).isPresent();
    }

    /**
     * {@inheritDoc}<br>
     * e.g. PostgreSQL执行获取一个游标类型的结果：
     * <blockquote>
     * <pre>
     *      {@link List}&lt;{@link DataRow}&gt; rows = {@link rabbit.sql.transaction.Tx}.using(() -&gt;
     *         baki.call("{call test.func2(:c::refcursor)}",
     *             Args.create("c",Param.IN_OUT("result", OUTParamType.REF_CURSOR))
     *             ).get(0));
     * </pre>
     * </blockquote>
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return DataRow
     */
    @Override
    public DataRow call(String name, Map<String, Param> args) {
        return executeCallStatement(name, args);
    }

    /**
     * {@inheritDoc}
     *
     * @return 数据源元信息
     * @throws RuntimeException 如果数据库关闭或者获取连接对象失败
     */
    @Override
    public DatabaseMetaData getMetaData() {
        try {
            if (metaData == null) {
                metaData = getConnection().getMetaData();
            }
            return metaData;
        } catch (SQLException throwables) {
            throw new RuntimeException("fail to get metadata: ", throwables);
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
    protected String prepareSql(String sql, Map<String, Object> args) {
        String trimEndedSql = SqlUtil.trimEnd(sql);
        if (sql.startsWith("&")) {
            if (sqlFileManager != null) {
                trimEndedSql = SqlUtil.trimEnd(sqlFileManager.get(sql.substring(1)));
            } else {
                throw new NullPointerException("can not find property 'sqlFileManager' or SQLFileManager object init failed!");
            }
        }
        return dynamicSql(trimEndedSql, args, strictDynamicSqlArg);
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
            log.error("fetch connection failed:{}", e.getMessage());
            return null;
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
}