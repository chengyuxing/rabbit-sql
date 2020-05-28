package rabbit.sql.dao;

import rabbit.sql.Light;
import rabbit.common.types.DataRow;
import rabbit.sql.datasource.DataSourceUtil;
import rabbit.sql.page.AbstractPageHelper;
import rabbit.sql.page.Pageable;
import rabbit.sql.support.ICondition;
import rabbit.sql.support.JdbcSupport;
import rabbit.sql.types.Ignore;
import rabbit.sql.types.Param;
import rabbit.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>如果配置了{@link SQLFileManager },则接口所有方法都可以通过 <b>&amp文件名.sql</b> 名来获取sql文件内的sql,通过<b>&amp;</b>
 * 前缀符号来判断如果是sql名则获取sql否则当作sql直接执行</p>
 * 指定sql名执行：
 * <blockquote>
 * <pre>light.query("&amp;data.query", r -&gt; r, {@link Params}.builder()
 *                 .put("id", {@link Param}.IN(3))
 *                 .build())
 *                 .forEach(System.out::println);</pre>
 * </blockquote>
 * 直接执行一句sql：
 * <blockquote>
 * <pre>light.query("select * from test.user where id = 3;", {@link DataRow}::toMap)
 *                 forEach(System.out::println)</pre>
 * </blockquote>
 *
 * @see rabbit.sql.support.JdbcSupport
 * @see rabbit.sql.Light
 */
public class LightDao extends JdbcSupport implements Light {
    private final static Logger log = LoggerFactory.getLogger(LightDao.class);
    private SQLFileManager sqlFileManager;
    private DataSource dataSource;
    private DatabaseMetaData metaData;

    LightDao() {

    }

    /**
     * 构造函数
     *
     * @param dataSource 数据源
     */
    public LightDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 实例化一个LightDao对象
     *
     * @param dataSource 数据源
     * @return LightDao实例
     */
    public static LightDao of(DataSource dataSource) {
        return new LightDao(dataSource);
    }

    /**
     * 指定sql文件解析管理器
     *
     * @param sqlFileManager sql文件解析管理器
     */
    public void setSqlFileManager(SQLFileManager sqlFileManager) {
        this.sqlFileManager = sqlFileManager;
        try {
            sqlFileManager.init();
        } catch (IOException e) {
            log.error("sql file is not exists:{}", e.getMessage());
        } catch (URISyntaxException e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public long execute(String sql) {
        return executeNonQuery(sql, Params.empty());
    }

    @Override
    public long execute(String sql, Map<String, Param> params) {
        return executeNonQuery(sql, params);
    }

    @Override
    public int insert(String tableName, Map<String, Param> data) {
        return insert(tableName, data, null);
    }

    @Override
    public int insert(String tableName, Map<String, Param> data, Ignore ignore) {
        return executeNonQuery(SqlUtil.generateInsert(tableName, data, ignore), data);
    }

    @Override
    public int insert(String tableName, DataRow row) {
        return insert(tableName, row, null);
    }

    @Override
    public int insert(String tableName, DataRow row, Ignore ignore) {
        return executeNonQueryOfDataRow(SqlUtil.generateInsert(tableName, row.toMap(Param::IN), ignore), row);
    }

    @Override
    public int insert(String tableName, Collection<Map<String, Param>> data) {
        if (data != null && data.size() > 0) {
            Map<String, Param> first = data.stream().findFirst().get();
            return executeNonQuery(SqlUtil.generateInsert(tableName, first, null), data);
        }
        return -1;
    }

    @Override
    public int delete(String tableName, ICondition ICondition) {
        return executeNonQuery("delete from " + tableName + " " + ICondition.getSql(), ICondition.getParams());
    }

    @Override
    public int update(String tableName, Map<String, Param> data, ICondition ICondition) {
        data.putAll(ICondition.getParams());
        return executeNonQuery(SqlUtil.generateUpdate(tableName, data) + ICondition.getSql(), data);
    }

    @Override
    public <T> Stream<T> query(String sql, Function<DataRow, T> convert) {
        return query(sql, convert, -1, null, null);
    }

    @Override
    public <T> Stream<T> query(String sql, Function<DataRow, T> convert, long fetchSize) {
        return query(sql, convert, fetchSize, null, null);
    }

    @Override
    public <T> Stream<T> query(String sql, Function<DataRow, T> convert, Map<String, Param> args) {
        return query(sql, convert, -1, args, null);
    }

    @Override
    public <T> Stream<T> query(String sql, Function<DataRow, T> convert, Map<String, Param> args, long fetchSize) {
        return query(sql, convert, fetchSize, args, null);
    }

    @Override
    public <T> Stream<T> query(String sql, Function<DataRow, T> convert, ICondition ICondition) {
        return query(sql, convert, -1, null, ICondition);
    }

    @Override
    public <T> Stream<T> query(String sql, Function<DataRow, T> convert, ICondition ICondition, long fetchSize) {
        return query(sql, convert, fetchSize, null, ICondition);
    }

    @Override
    public <T> Pageable<T> query(String recordQuery, String countQuery, Function<DataRow, T> convert, Map<String, Param> args, AbstractPageHelper pager) {
        return fetch(countQuery, r -> r.getInt(0), args).map(cn -> {
            pager.init(cn);
            List<T> data = query(pager.wrapPagedSql(getSql(recordQuery)), convert, args).collect(Collectors.toList());
            return Pageable.of(pager, data);
        }).orElseGet(Pageable::empty);
    }

    @Override
    public <T> Pageable<T> query(String recordQuery, String countQuery, Function<DataRow, T> convert, ICondition ICondition, AbstractPageHelper page) {
        String cnd = ICondition.getSql();
        String countCnd = cnd;
        int orderExist = cnd.lastIndexOf("order by");
        if (orderExist != -1) {
            countCnd = cnd.substring(0, cnd.lastIndexOf("order by"));
        }
        Map<String, Param> paramMap = ICondition.getParams();
        return query(getSql(recordQuery) + cnd, getSql(countQuery) + countCnd, convert, paramMap, page);
    }

    @Override
    public <T> Pageable<T> query(String recordQuery, Function<DataRow, T> convert, Map<String, Param> args, AbstractPageHelper page) {
        String query = getSql(recordQuery);
        String countQuery = "SELECT COUNT(*) " + query.substring(query.toLowerCase().lastIndexOf("from"));
        return query(query, countQuery, convert, args, page);
    }

    @Override
    public <T> Pageable<T> query(String recordQuery, Function<DataRow, T> convert, AbstractPageHelper page) {
        String query = getSql(recordQuery);
        String countQuery = "SELECT COUNT(*) " + query.substring(query.toLowerCase().lastIndexOf("from"));
        return query(query, countQuery, convert, Params.empty(), page);
    }

    @Override
    public <T> Pageable<T> query(String recordQuery, Function<DataRow, T> convert, ICondition ICondition, AbstractPageHelper page) {
        String query = getSql(recordQuery);
        String countQuery = "SELECT COUNT(*) " + query.substring(query.toLowerCase().lastIndexOf("from"));
        return query(query, countQuery, convert, ICondition, page);
    }

    @Override
    public <T> Optional<T> fetch(String sql, Function<DataRow, T> convert) {
        return query(sql, convert, 1).findFirst();
    }

    @Override
    public <T> Optional<T> fetch(String sql, Function<DataRow, T> convert, ICondition ICondition) {
        return query(sql, convert, ICondition, 1).findFirst();
    }

    @Override
    public <T> Optional<T> fetch(String sql, Function<DataRow, T> convert, Map<String, Param> args) {
        return query(sql, convert, args, 1).findFirst();
    }

    @Override
    public boolean exists(String sql) {
        return fetch(sql, r -> r).isPresent();
    }

    @Override
    public boolean exists(String sql, ICondition ICondition) {
        return fetch(sql, r -> r, ICondition).isPresent();
    }

    @Override
    public boolean exists(String sql, Map<String, Param> args) {
        return fetch(sql, r -> r, args).isPresent();
    }

    /**
     * 执行一个存储过程<br>
     * PostgreSQL执行获取一个游标类型的结果：
     * <blockquote>
     * <pre>
     *  {@link Stream}&lt;{@link DataRow}&gt; rows = {@link rabbit.sql.transaction.Tx}.using(() -&gt;
     *    light.function("call test.func2(:c::refcursor)",
     *       Params.builder()
     *         .put("c",Param.IN_OUT("result", OUTParamType.REF_CURSOR))
     *         .build())
     *         .get(0);
     *       );
     *   </pre>
     * </blockquote>
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return 包含至少一个结果的DataRow结果集
     */
    @Override
    public DataRow procedure(String name, Map<String, Param> args) {
        return executeCall("{" + name + "}", args);
    }

    /**
     * 执行一个函数<br>
     * 同{@code procedure(String name, Map<String, Param> args)}方法
     *
     * @param name 函数名
     * @param args 参数（占位符名字，参数对象）
     * @return 包含至少一个结果的DataRow结果集
     */
    @Override
    public DataRow function(String name, Map<String, Param> args) {
        return procedure(name, args);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (metaData == null) {
            metaData = getConnection().getMetaData();
        }
        return metaData;
    }

    /**
     * 如果使用取地址符"&amp;sql文件名.sql名"则获取sql文件中已加载的sql
     *
     * @param sql sql或sql名
     * @return sql
     */
    @Override
    protected String getSql(String sql) {
        if (sql.startsWith("&")) {
            if (sqlFileManager != null) {
                try {
                    return sqlFileManager.get(sql.substring(1));
                } catch (IOException | URISyntaxException e) {
                    log.error("get SQL failed:{}", e.getMessage());
                }
            }
            throw new NullPointerException("can not find property 'sqlPath' or SQLFileManager init failed!");
        }
        return sql;
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

    /**
     * 释放连接对象，如果有事务存在，并不会执行真正的释放
     *
     * @param connection 连接对象
     * @param dataSource 数据源
     */
    @Override
    protected void releaseConnection(Connection connection, DataSource dataSource) {
        DataSourceUtil.releaseConnectionIfNecessary(connection, dataSource);
    }
}