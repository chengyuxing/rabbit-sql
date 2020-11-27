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
    private SQLFileManager sqlFileManager;
    private DatabaseMetaData metaData;

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
            sqlFileManager.init();
        } catch (IOException e) {
            log.error("sql file is not exists:{}", e.getMessage());
        } catch (URISyntaxException e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public DataRow execute(String sql) {
        return executeAny(sql, Collections.emptyMap());
    }

    @Override
    public DataRow execute(String sql, Map<String, Object> args) {
        return executeAny(sql, args);
    }

    @Override
    public int insert(DataFrame dataFrame) {
        Collection<Map<String, Object>> data = dataFrame.getRows();
        Iterator<Map<String, Object>> iterator = data.iterator();
        if (iterator.hasNext()) {
            Map<String, Object> first = new HashMap<>(iterator.next());
            List<String> tableFields = Collections.emptyList();
            if (!dataFrame.isStrict()) {
                log.debug("prepare for non-strict insert...");
                tableFields = execute(dataFrame.getTableFieldsSql(), sc -> {
                    sc.executeQuery();
                    ResultSet fieldsResultSet = sc.getResultSet();
                    List<String> fields = Arrays.asList(JdbcUtil.createNames(fieldsResultSet, ""));
                    JdbcUtil.closeResultSet(fieldsResultSet);
                    log.debug("all fields of table: {} {}", dataFrame.getTableName(), fields);
                    return fields;
                });
            }
            String insertSql = SqlUtil.generateInsert(dataFrame.getTableName(), first, dataFrame.getIgnore(), tableFields);
            return executeNonQuery(insertSql, data);
        }
        return -1;
    }

    @Override
    public int delete(String tableName, ICondition ICondition) {
        return executeNonQuery("delete from " + tableName + " " + ICondition.getSql(), Collections.singletonList(ICondition.getArgs()));
    }

    @Override
    public int update(String tableName, Map<String, Object> data, ICondition ICondition) {
        String update = SqlUtil.generateUpdate(tableName, data);
        data.putAll(ICondition.getArgs());
        return executeNonQuery(update + ICondition.getSql(), Collections.singletonList(data));
    }

    @Override
    public Stream<DataRow> query(String sql) {
        return query(sql, Args.create());
    }

    @Override
    public Stream<DataRow> query(String sql, Map<String, Object> args) {
        try {
            return executeQueryStream(sql, args);
        } catch (SQLException ex) {
            log.error(ex.toString());
        }
        return Stream.empty();
    }

    @Override
    public <T> IPageable<T> query(String recordQuery, int page, int size) {
        return new Pageable<>(this, recordQuery, page, size);
    }

    @Override
    public Optional<DataRow> fetch(String sql) {
        return fetch(sql, Args.create());
    }

    @Override
    public Optional<DataRow> fetch(String sql, Map<String, Object> args) {
        try (Stream<DataRow> s = query(sql, args)) {
            return s.findFirst();
        }
    }

    @Override
    public boolean exists(String sql) {
        return fetch(sql).isPresent();
    }

    @Override
    public boolean exists(String sql, Map<String, Object> args) {
        return fetch(sql, args).isPresent();
    }

    /**
     * 执行一个存储过程或函数<br>
     * PostgreSQL执行获取一个游标类型的结果：
     * <blockquote>
     * <pre>
     *  {@link List}&lt;{@link DataRow}&gt; rows = {@link rabbit.sql.transaction.Tx}.using(() -&gt;
     *    baki.function("{call test.func2(:c::refcursor)}",
     *       Args.create("c",Param.IN_OUT("result", OUTParamType.REF_CURSOR))
     *       ).get(0));
     *   </pre>
     * </blockquote>
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return 包含至少一个结果的DataRow结果集
     */
    @Override
    public DataRow call(String name, Map<String, Param> args) {
        return executeCall(name, args);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (metaData == null) {
            metaData = getConnection().getMetaData();
        }
        return metaData;
    }

    /**
     * 如果使用取地址符"&amp;sql文件名.sql名"则获取sql文件中已缓存的sql
     *
     * @param sql sql或sql名
     * @return sql
     */
    @Override
    protected String prepareSql(String sql, Map<String, Object> args) {
        String trimEndedSql = SqlUtil.trimEnd(sql);
        if (sql.startsWith("&")) {
            if (sqlFileManager != null) {
                try {
                    trimEndedSql = SqlUtil.trimEnd(sqlFileManager.get(sql.substring(1)));
                } catch (IOException | URISyntaxException e) {
                    log.error("get SQL failed:{}", e.getMessage());
                }
            } else {
                throw new NullPointerException("can not find property 'sqlPath' or SQLFileManager init failed!");
            }
        }
        return dynamicSql(trimEndedSql, args);
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