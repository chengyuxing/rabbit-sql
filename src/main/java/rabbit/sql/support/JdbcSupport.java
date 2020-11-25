package rabbit.sql.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.common.tuple.Pair;
import rabbit.common.types.DataRow;
import rabbit.common.types.UncheckedCloseable;
import rabbit.sql.types.Param;
import rabbit.sql.types.ParamMode;
import rabbit.sql.utils.JdbcUtil;
import rabbit.sql.utils.SqlUtil;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * <h2>jdbc基本操作支持</h2><br>
 * <p>:name (jdbc标准的传名参数写法，参数将被预编译安全处理)</p><br>
 * <p>${part} (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)</p><br>
 * <p>小提示：PostgreSQL中，带有问号的操作符(?,?|,?&amp;,@?)可以使用双问号(??,??|,??&amp;,@??)解决预编译sql参数未设定的报错，或者直接使用函数</p><br>
 * 执行的SQL字符串例如：
 * <blockquote>
 * <pre>
 *       select t.id || 'number' || 'name:cyx','{"name": "user"}'::jsonb
 *       from test.user t
 *       where id = :id::integer --后缀类型转换
 *       and id {@code >} :idc
 *       and name = text :username --前缀类型转换
 *       and '["a","b","c"]'::jsonb{@code ??&} array ['a', 'b'] ${cnd};
 *     </pre>
 * </blockquote>
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
     *
     * @param connection 连接对象
     * @param dataSource 数据源
     */
    protected abstract void releaseConnection(Connection connection, DataSource dataSource);

    /**
     * 提供一个抽象方法供实现类对单前要执行的sql做一些准备操作
     *
     * @param sql  sql
     * @param args 参数
     * @return 处理后的sql
     */
    protected abstract String prepareSql(String sql, Map<String, Object> args);

    /**
     * 获取经过解析处理后的sql
     *
     * @param sql  sql
     * @param args 参数
     * @return 最终要执行的sql
     */
    private String getSourceSql(String sql, Map<String, Object> args) {
        String preparedSql = prepareSql(sql, args);
        return SqlUtil.resolveSqlPart(preparedSql, args);
    }

    /**
     * 执行一句sql
     *
     * @param sql      sql
     * @param callback 执行声明回调函数
     * @param <T>      结果类型参数
     * @return 任意类型
     */
    public <T> T execute(final String sql, StatementCallback<T> callback) {
        PreparedStatement statement = null;
        Connection connection = getConnection();
        try {
            statement = connection.prepareStatement(sql);
            return callback.doInStatement(statement);
        } catch (SQLException e) {
            JdbcUtil.closeStatement(statement);
            statement = null;
            releaseConnection(connection, getDataSource());
            throw new RuntimeException("execute sql [" + sql + "] error: ", e);
        } finally {
            JdbcUtil.closeStatement(statement);
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * 执行query语句，ddl或dml语句
     *
     * @param sql  sql
     * @param args 参数
     * @return 包含各种结果的行数据类型
     */
    public DataRow executeAny(final String sql, Map<String, Object> args) {
        String sourceSql = sql;
        boolean hasArgs = args != null && !args.isEmpty();
        if (hasArgs) {
            sourceSql = getSourceSql(sql, args);
        }
        log.debug("SQL:{}", sourceSql);
        log.debug("Args:{}", args);

        Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql);
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        final String preparedSql = preparedSqlAndArgNames.getItem1();

        return execute(preparedSql, sc -> {
            if (hasArgs) {
                JdbcUtil.setSqlArgs(sc, args, argNames);
            }
            boolean isQuery = sc.execute();
            DataRow result;
            if (isQuery) {
                List<DataRow> rows = JdbcUtil.createDataRows(sc.getResultSet(), preparedSql, -1);
                result = DataRow.of(new String[]{"result", "type"},
                        new String[]{"java.util.ArrayList<DataRow>", "java.lang.String"},
                        new Object[]{rows, "query"});
            } else {
                int count = sc.getUpdateCount();
                if (count <= 0) {
                    result = DataRow.fromPair("result", 0, "type", "DDL statement");
                } else {
                    result = DataRow.fromPair("result", count, "type", "DML statement");
                }
            }
            return result;
        });
    }

    /**
     * 惰性执行一句查询，只有调用终端操作和短路操作才会真正开始执行<br>
     * 使用完请务必关闭流，否则将一直占用连接对象直到连接池耗尽<br>
     * 使用{@code try-with-resource}进行包裹：
     * <blockquote>
     * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; stream = ...) {
     *       stream.limit(10).forEach(System.out::println);
     *         }</pre>
     * </blockquote>
     *
     * @param sql  e.g. <code>select * from test.user where id = :id</code>
     * @param args 参数 （占位符名字，参数对象）
     * @return Stream数据流
     * @throws SQLException sqlEx
     */
    public Stream<DataRow> executeQueryStream(final String sql, Map<String, Object> args) throws SQLException {
        UncheckedCloseable close = null;
        try {
            if (args == null) {
                args = Collections.emptyMap();
            }
            String sourceSql = getSourceSql(sql, args);
            log.debug("SQL:{}", sourceSql);
            log.debug("Args:{}", args);

            Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql);
            final List<String> argNames = preparedSqlAndArgNames.getItem2();
            final String preparedSql = preparedSqlAndArgNames.getItem1();

            Connection connection = getConnection();
            close = UncheckedCloseable.wrap(connection);
            PreparedStatement statement = connection.prepareStatement(preparedSql);
            JdbcUtil.setSqlArgs(statement, args, argNames);
            close = close.nest(statement);
            ResultSet resultSet = statement.executeQuery();
            close = close.nest(resultSet);
            return StreamSupport.stream(new Spliterators.AbstractSpliterator<DataRow>(Long.MAX_VALUE, Spliterator.ORDERED) {
                String[] names = null;

                @Override
                public boolean tryAdvance(Consumer<? super DataRow> action) {
                    try {
                        if (!resultSet.next()) {
                            return false;
                        }
                        if (names == null) {
                            names = JdbcUtil.createNames(resultSet, preparedSql);
                        }
                        action.accept(JdbcUtil.createDataRow(names, resultSet));
                        return true;
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }, false).onClose(close);
        } catch (SQLException sqlEx) {
            if (close != null) {
                try {
                    close.close();
                } catch (Exception e) {
                    sqlEx.addSuppressed(e);
                }
            }
            throw sqlEx;
        }
    }

    /**
     * 执行一句非查询语句(insert，update，delete)<br>
     * e.g.
     * <blockquote>
     * <pre>insert into table (a,b,c) values (:v1,:v2,:v3)</pre>
     * </blockquote>
     *
     * @param sql  sql
     * @param args 数据
     * @return 总的受影响的行数
     */
    public int executeNonQuery(final String sql, final Collection<Map<String, Object>> args) {
        String sourceSql = sql;
        boolean hasArgs = args != null && !args.isEmpty();
        if (hasArgs) {
            Map<String, Object> firstArg = args.stream().findFirst().get();
            sourceSql = getSourceSql(sql, firstArg);
        }
        log.debug("SQL:{}", sourceSql);
        log.debug("Args:{}", args);

        Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql);
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        final String preparedSql = preparedSqlAndArgNames.getItem1();

        return execute(preparedSql, sc -> {
            int i = 0;
            if (hasArgs) {
                for (Map<String, Object> arg : args) {
                    JdbcUtil.setSqlArgs(sc, arg, argNames);
                    i += sc.executeUpdate();
                }
            } else {
                i = sc.executeUpdate();
            }
            log.debug("{} rows updated!", i);
            return i;
        });
    }

    /**
     * 执行存储过程或函数<br>
     * 所有出参结果都放入到{@link DataRow}中，可通过命名参数名来取得，或者通过索引来取，索引从0开始<br>
     * 语句形如原生jdbc，只是将?号改为命名参数（:参数名）：
     * <blockquote>
     * <pre>
     *         {call test.func1(:arg1, :arg2, :result1, :result2)};
     *         {call test.func2(:result::refcursor)}; //PostgreSQL
     *         {:result = call test.func3()};
     *     </pre>
     * </blockquote>
     *
     * @param procedure 存储过程名
     * @param args      参数
     * @return DataRow
     */
    public DataRow executeCall(final String procedure, Map<String, Param> args) {
        String sourceSql = procedure;
        boolean hasArgs = args != null && !args.isEmpty();
        if (hasArgs) {
            Map<String, Object> pArgs = new HashMap<>();
            args.forEach((k, v) -> pArgs.put(k, v.getValue()));
            sourceSql = getSourceSql(procedure, pArgs);
        }
        log.debug("Procedure:{}", sourceSql);
        log.debug("Args:{}", args);

        Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql);
        final String executeSql = preparedSqlAndArgNames.getItem1();
        final List<String> argNames = preparedSqlAndArgNames.getItem2();

        CallableStatement statement = null;
        Connection connection = getConnection();
        try {
            if (hasArgs) {
                statement = connection.prepareCall(executeSql);
                JdbcUtil.setStoreArgs(statement, args, argNames);
                statement.execute();

                List<String> outNames = new ArrayList<>();
                for (String name : argNames) {
                    ParamMode mode = args.get(name).getParamMode();
                    if (mode == ParamMode.OUT || mode == ParamMode.IN_OUT) {
                        outNames.add(name);
                    }
                }
                if (outNames.size() > 0) {
                    Object[] values = new Object[outNames.size()];
                    String[] types = new String[values.length];
                    int resultIndex = 0;
                    for (int i = 0; i < argNames.size(); i++) {
                        if (args.get(argNames.get(i)).getParamMode() == ParamMode.OUT || args.get(argNames.get(i)).getParamMode() == ParamMode.IN_OUT) {
                            Object result = statement.getObject(i + 1);
                            if (null == result) {
                                values[resultIndex] = null;
                                types[resultIndex] = null;
                            } else if (result instanceof ResultSet) {
                                List<DataRow> rows = JdbcUtil.createDataRows((ResultSet) result, executeSql, -1);
                                values[resultIndex] = rows;
                                types[resultIndex] = "java.util.ArrayList<DataRow>";
                                log.debug("boxing a result with type: cursor, convert to ArrayList<DataRow>, get result by name:{} or index:{}!", outNames.get(resultIndex), resultIndex);
                            } else {
                                values[resultIndex] = result;
                                types[resultIndex] = result.getClass().getName();
                                log.debug("boxing a result with type:{}, get result by name:{} or index:{}!", types[resultIndex], outNames.get(resultIndex), resultIndex);
                            }
                            resultIndex++;
                        }
                    }
                    return DataRow.of(outNames.toArray(new String[0]), types, values);
                }
            }
            return DataRow.empty();
        } catch (SQLException e) {
            JdbcUtil.closeStatement(statement);
            statement = null;
            releaseConnection(connection, getDataSource());
            throw new RuntimeException("execute procedure [" + procedure + "] error:", e);
        } finally {
            JdbcUtil.closeStatement(statement);
            releaseConnection(connection, getDataSource());
        }
    }
}
