package rabbit.sql.dao;

import rabbit.sql.Light;
import rabbit.common.types.DataRow;
import rabbit.sql.datasource.DataSourceUtil;
import rabbit.sql.page.AbstractPageHelper;
import rabbit.sql.page.Pageable;
import rabbit.sql.support.ICondition;
import rabbit.sql.support.JdbcSupport;
import rabbit.sql.support.SQLFileManager;
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
 * 默认的light实现
 */
public class LightDao extends JdbcSupport implements Light {
    private final static Logger log = LoggerFactory.getLogger(LightDao.class);
    private SQLFileManager sqlFileManager;
    private DataSource dataSource;
    private DatabaseMetaData metaData;

    LightDao() {

    }

    public LightDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static LightDao of(DataSource dataSource) {
        return new LightDao(dataSource);
    }

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
        return executeNonQuery(SqlUtil.generateInsert(tableName, data), data);
    }

    @Override
    public int insert(String tableName, DataRow row) {
        return executeNonQueryOfDataRow(SqlUtil.generateInsert(tableName, row), row);
    }

    @Override
    public int insert(String tableName, Collection<Map<String, Param>> data) {
        if (data != null && data.size() > 0) {
            Map<String, Param> first = data.stream().findFirst().get();
            return executeNonQuery(SqlUtil.generateInsert(tableName, first), data);
        }
        return -1;
    }

    @Override
    public int delete(String tableName, ICondition ICondition) {
        return executeNonQuery("delete from " + tableName + " " + ICondition.getString(), ICondition.getParams());
    }

    @Override
    public int update(String tableName, Map<String, Param> data, ICondition ICondition) {
        data.putAll(ICondition.getParams());
        return executeNonQuery(SqlUtil.generateUpdate(tableName, data) + ICondition.getString(), data);
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
        String cnd = ICondition.getString();
        Map<String, Param> paramMap = ICondition.getParams();
        return query(getSql(recordQuery) + cnd, getSql(countQuery) + cnd, convert, paramMap, page);
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

    @Override
    public DataRow call(String name, Map<String, Param> args) {
        return executeCall("{" + name + "}", args);
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

    @Override
    protected void releaseConnection(Connection connection, DataSource dataSource) {
        DataSourceUtil.releaseConnectionIfNecessary(connection, dataSource);
    }
}