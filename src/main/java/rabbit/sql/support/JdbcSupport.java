package rabbit.sql.support;

import rabbit.sql.utils.JdbcUtil;
import rabbit.sql.utils.SqlUtil;
import rabbit.common.tuple.Pair;
import rabbit.common.types.DataRow;
import rabbit.sql.types.Param;
import rabbit.sql.types.ParamMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * jdbc基本操作支持
 */
public abstract class JdbcSupport {
    private final static Logger log = LoggerFactory.getLogger(JdbcSupport.class);

    /**
     * 设置数据源
     *
     * @return 数据源
     */
    protected abstract DataSource getDataSource();

    /**
     * 获取连接对象
     *
     * @return 连接对象
     */
    protected abstract Connection getConnection();

    /**
     * 释放连接的实现逻辑，比如在开启事务同步时，应保持连接，
     * 而不应该释放，在执行其他非事务操作完成时应立即释放
     */
    protected abstract void releaseConnection(Connection connection, DataSource dataSource);

    /**
     * 提供一个抽象方法供实现类对单前要执行的sql做一些准备操作
     *
     * @param sql sql
     * @return 处理后的sql
     */
    protected abstract String getSql(String sql);

    public <T> T execute(StatementCallback<T> callback) {
        CallableStatement statement = null;
        String executeSql = "";
        Connection connection = getConnection();
        try {
            executeSql = callback.getExecuteSql();
            statement = connection.prepareCall(executeSql);
            return callback.doInStatement(statement);
        } catch (SQLException e) {
            JdbcUtil.closeStatement(statement);
            statement = null;
            releaseConnection(connection, getDataSource());
            throw new RuntimeException(String.format("execute target sql[%s]:%s", executeSql, e.getMessage()));
        } finally {
            JdbcUtil.closeStatement(statement);
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * 批量执行 insert，update，delete（e.g. insert into table (a,b,c) values (:v1,:v2,:v3)）
     *
     * @param sql  sql
     * @param args 数据
     * @return 总的受影响的行数
     */
    public int executeNonQuery(final String sql, final Collection<Map<String, Param>> args) {
        class BatchUpdateStatementCallback implements StatementCallback<Integer> {
            private List<String> argNames = new ArrayList<>();

            @Override
            public Integer doInStatement(CallableStatement statement) throws SQLException {
                int i = 0;
                for (Map<String, Param> arg : args) {
                    JdbcUtil.registerParams(statement, arg, argNames);
                    i += statement.executeUpdate();
                }
                log.info("{} rows updated!", i);
                return i;
            }

            @Override
            public String getExecuteSql() {
                if (args == null || args.size() < 1) {
                    throw new NoSuchElementException("args is null or length less than 1.");
                }
                Map<String, Param> firstArg = args.stream().findFirst().get();
                String sourceSql = SqlUtil.resolveSqlPart(getSql(sql), firstArg);
                log.debug("SQL:{}", sourceSql);
                log.debug("Args:{}", args);

                Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSqlAndIndexedArgNames(sourceSql);
                argNames = preparedSqlAndArgNames.getItem2();
                return preparedSqlAndArgNames.getItem1();
            }
        }
        return execute(new BatchUpdateStatementCallback());
    }

    public int executeNonQuery(final String sql, final Map<String, Param> args) {
        return executeNonQuery(sql, Collections.singletonList(args));
    }

    public int executeNonQueryOfDataRow(final String sql, final Collection<DataRow> args) {
        class RowUpdateStatementCallback implements StatementCallback<Integer> {
            private List<String> argNames = new ArrayList<>();

            @Override
            public Integer doInStatement(CallableStatement statement) throws SQLException {
                int i = 0;
                for (DataRow row : args) {
                    Map<String, Param> arg = row.toMap(Param::IN);
                    JdbcUtil.registerParams(statement, arg, argNames);
                    i += statement.executeUpdate();
                }
                log.info("{} rows updated!", i);
                return i;
            }

            @Override
            public String getExecuteSql() {
                if (args == null || args.size() < 1) {
                    throw new NoSuchElementException("args is null or length less than 1.");
                }
                Map<String, Param> firstArg = args.stream()
                        .map(r -> r.toMap(Param::IN))
                        .findFirst().get();
                String sourceSql = SqlUtil.resolveSqlPart(getSql(sql), firstArg);
                log.debug("SQL:{}", sourceSql);
                log.debug("Args:{}", args);

                Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSqlAndIndexedArgNames(sourceSql);
                argNames = preparedSqlAndArgNames.getItem2();
                return preparedSqlAndArgNames.getItem1();
            }
        }
        return execute(new RowUpdateStatementCallback());
    }

    public int executeNonQueryOfDataRow(final String sql, final DataRow args) {
        return executeNonQueryOfDataRow(sql, Collections.singletonList(args));
    }

    /**
     * 查询
     *
     * @param sql        sql
     * @param convert    类型转换
     * @param fetchSize  请求条数
     * @param args       参数 （占位符名字，参数对象）
     * @param ICondition 条件
     * @param <T>        类型参数
     * @return 结果集
     */
    public <T> Stream<T> query(final String sql, Function<DataRow, T> convert, final long fetchSize, Map<String, Param> args, ICondition ICondition) {
        class QueryStatementCallback implements StatementCallback<Stream<T>> {
            private final Map<String, Param> params = new HashMap<>();
            private List<String> argNames = new ArrayList<>();

            @Override
            public Stream<T> doInStatement(CallableStatement statement) throws SQLException {
                JdbcUtil.registerParams(statement, params, argNames);
                ResultSet resultSet = statement.executeQuery();
                return JdbcUtil.resolveResultSet(resultSet, fetchSize, convert);
            }

            @Override
            public String getExecuteSql() {
                String sourceSql = getSql(sql);
                if (args != null)
                    params.putAll(args);
                if (ICondition != null) {
                    params.putAll(ICondition.getParams());
                    sourceSql += ICondition.getString();
                }
                sourceSql = SqlUtil.resolveSqlPart(sourceSql, params);
                log.debug("SQL：{}", sourceSql);
                log.debug("Args:{}", params);

                Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSqlAndIndexedArgNames(sourceSql);
                argNames = preparedSqlAndArgNames.getItem2();
                return preparedSqlAndArgNames.getItem1();
            }
        }
        return execute(new QueryStatementCallback());
    }

    /**
     * 执行存储过程或函数
     *
     * @param sql  sql
     * @param args 参数
     * @return DataRow
     */
    public DataRow executeCall(final String sql, Map<String, Param> args) {
        class ProcedureStatementCallback implements StatementCallback<DataRow> {
            private List<String> argNames = new ArrayList<>();

            @Override
            public DataRow doInStatement(CallableStatement statement) throws SQLException {
                JdbcUtil.registerParams(statement, args, argNames);
                statement.execute();
                Param[] params = args.values().toArray(new Param[0]);
                String[] fields = new String[params.length];
                Object[] values = new Object[params.length];
                String[] types = new String[params.length];
                for (int i = 0; i < params.length; i++) {
                    if (params[i].getParamMode() == ParamMode.OUT || params[i].getParamMode() == ParamMode.IN_OUT) {
                        Object result = statement.getObject(i + 1);
                        fields[i] = "column" + i;
                        if (result instanceof ResultSet) {
                            Stream<DataRow> rowStream = JdbcUtil.resolveResultSet((ResultSet) result, -1, row -> row);
                            values[i] = rowStream;
                            types[i] = "java.util.stream.Stream<DataRow>";
                            log.info("boxing a result with type: cursor,convert to Stream<DataRow>,get result by name:{} or index:{}!", fields[i], i);
                        } else {
                            values[i] = result;
                            types[i] = result.getClass().getName();
                            log.info("boxing a result with type:{},get result by name:{} or index:{}!", types[i], fields[i], i);
                        }
                    }
                }
                return DataRow.of(fields, types, values);
            }

            @Override
            public String getExecuteSql() {
                String sourceSql = SqlUtil.resolveSqlPart(getSql(sql), args);
                log.debug("SQL：{}", sourceSql);
                log.debug("Args：{}", args);

                Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSqlAndIndexedArgNames(sourceSql);
                String executeSql = preparedSqlAndArgNames.getItem1();
                argNames = preparedSqlAndArgNames.getItem2();
                return executeSql;
            }
        }
        return execute(new ProcedureStatementCallback());
    }
}
