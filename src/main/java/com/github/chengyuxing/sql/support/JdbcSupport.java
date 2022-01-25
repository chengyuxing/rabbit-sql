package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.sql.datasource.AbstractTransactionSyncManager;
import com.github.chengyuxing.sql.exceptions.SqlRuntimeException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.SqlUtil;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
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
    protected abstract String prepareSql(String sql, Map<String, ?> args);

    /**
     * 是否检查预编译sql对应的参数类型
     *
     * @return 是否检查
     */
    protected abstract boolean checkParameterType();

    /**
     * 执行一句预编译的sql
     *
     * @param sql      sql
     * @param callback 执行声明回调函数
     * @param <T>      结果类型参数
     * @return 任意类型
     * @throws SqlRuntimeException sql执行过程中出现异常
     */
    protected <T> T execute(final String sql, StatementCallback<T> callback) {
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            connection = getConnection();
            statement = connection.prepareStatement(sql);
            return callback.doInStatement(statement);
        } catch (SQLException e) {
            JdbcUtil.closeStatement(statement);
            statement = null;
            releaseConnection(connection, getDataSource());
            throw new SqlRuntimeException("execute sql:\n[" + sql + "]\nerror: ", e);
        } finally {
            JdbcUtil.closeStatement(statement);
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * 执行query、ddl、dml或plsql语句<br>
     * 返回数据为:<br>
     * 执行结果：{@code DataRow.get(0)} 或 {@code DataRow.get("result")}<br>
     * 执行类型：{@code DataRow.get(1)} 或 {@code DataRow.getString("type")}
     *
     * @param sql  原始sql
     * @param args 参数
     * @return 查询语句返回List，DML语句返回受影响的行数，DDL语句返回0
     * @throws SqlRuntimeException sql执行过程中出现错误
     */
    public DataRow execute(final String sql, Map<String, ?> args) {
        String sourceSql = prepareSql(sql, args);
        log.debug("SQL:{}", SqlUtil.highlightSql(sourceSql));
        log.debug("Args:{}", args);

        Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql, args);
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        final String preparedSql = preparedSqlAndArgNames.getItem1();

        return execute(preparedSql, sc -> {
            if (args != null && !args.isEmpty()) {
                JdbcUtil.setSqlTypedArgs(sc, checkParameterType(), args, argNames);
            }
            boolean isQuery = sc.execute();
            printSqlConsole(sc);
            DataRow result;
            if (isQuery) {
                List<DataRow> rows = JdbcUtil.createDataRows(sc.getResultSet(), preparedSql, -1);
                if (rows.size() == 1) {
                    result = DataRow.fromPair("result", rows.get(0), "type", "QUERY statement");
                } else {
                    result = DataRow.of(new String[]{"result", "type"}, new Object[]{rows, "QUERY statement"});
                }
            } else {
                int count = sc.getUpdateCount();
                result = DataRow.fromPair("result", count, "type", "DD(M)L statement");
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
     * @throws SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误.
     */
    public Stream<DataRow> executeQueryStream(final String sql, Map<String, ?> args) {
        if (args == null) {
            args = Collections.emptyMap();
        }
        String sourceSql = prepareSql(sql, args);
        log.debug("SQL:{}", SqlUtil.highlightSql(sourceSql));
        log.debug("Args:{}", args);

        Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql, args);
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        final String preparedSql = preparedSqlAndArgNames.getItem1();

        UncheckedCloseable close = null;
        try {
            Connection connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            if (!AbstractTransactionSyncManager.isTransactionActive()) {
                close = UncheckedCloseable.wrap(connection);
            }
            PreparedStatement statement = connection.prepareStatement(preparedSql);
            JdbcUtil.setSqlTypedArgs(statement, checkParameterType(), args, argNames);
            // if close is null. it means this query in transaction currently,
            // it's connection managed by Tx(transaction)
            // connection will not be close when read stream to the end in 'try-with-resource' block
            // or Stream.close() method.
            if (close == null) {
                log.warn(Printer.colorful("Stream Query in transaction now, I don't recommend it!!!, maybe you should optimize your program.", Color.YELLOW));
                close = UncheckedCloseable.wrap(statement);
            } else {
                close = close.nest(statement);
            }
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
                        throw new SqlRuntimeException("reading result set of query:[" + preparedSql + "]\nerror: ", ex);
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
            throw new SqlRuntimeException("\nexecute sql:[" + preparedSql + "]\nargs:" + args + "\nerror: ", sqlEx);
        }
    }

    /**
     * 批量执行非查询(ddl,dml)语句
     *
     * @param sqls 一组sql
     * @return 每条sql的执行结果
     * @throws SqlRuntimeException           执行批量操作时发生错误
     * @throws UnsupportedOperationException 数据库或驱动版本不支持批量操作
     * @throws IllegalArgumentException      如果执行的sql条数少1条
     */
    public int[] batchExecute(final String... sqls) {
        if (sqls.length > 0) {
            Statement statement = null;
            Connection connection = getConnection();
            if (JdbcUtil.supportsBatchUpdates(connection)) {
                try {
                    statement = connection.createStatement();
                    for (String sql : sqls) {
                        statement.addBatch(prepareSql(sql, Collections.emptyMap()));
                    }
                    return statement.executeBatch();
                } catch (SQLException e) {
                    JdbcUtil.closeStatement(statement);
                    statement = null;
                    releaseConnection(connection, getDataSource());
                    throw new SqlRuntimeException("execute batch error: ", e);
                } finally {
                    JdbcUtil.closeStatement(statement);
                    releaseConnection(connection, getDataSource());
                }
            }
            throw new UnsupportedOperationException("your database or jdbc driver not support batch execute currently!");
        }
        throw new IllegalArgumentException("must be no less than one SQL.");
    }

    /**
     * 批量执行一句非查询语句(insert，update，delete)<br>
     * e.g.
     * <blockquote>
     * <pre>insert into table (a,b,c) values (:v1,:v2,:v3)</pre>
     * <pre>[{v1:'a',v2:'b',v3:'c'},{...},...]</pre>
     * </blockquote>
     *
     * @param sql  sql
     * @param args 数据 --每行数据类型和参数个数都必须相同
     * @return 总的受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误
     */
    public int executeNonQuery(final String sql, final Collection<? extends Map<String, ?>> args) {
        String sourceSql = sql;
        Map<String, ?> firstArg = Collections.emptyMap();
        boolean hasArgs = args != null && !args.isEmpty();
        if (hasArgs) {
            firstArg = args.iterator().next();
            sourceSql = prepareSql(sql, firstArg);
        }
        log.debug("SQL:{}", SqlUtil.highlightSql(sourceSql));
        if (hasArgs) {
            if (args.size() == 1) {
                log.debug("Args:{}", args);
            } else {
                log.debug("Args:{},...", firstArg);
            }
        }
        Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql, firstArg);
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        final String preparedSql = preparedSqlAndArgNames.getItem1();

        return execute(preparedSql, sc -> {
            int i = 0;
            if (hasArgs) {
                for (Map<String, ?> arg : args) {
                    JdbcUtil.setSqlTypedArgs(sc, checkParameterType(), arg, argNames);
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
     * 执行一句非查询语句(insert，update，delete)<br>
     * e.g.
     * <blockquote>
     * <pre>insert into table (a,b,c) values (:v1,:v2,:v3)</pre>
     * </blockquote>
     *
     * @param sql sql
     * @param arg 数据
     * @return 总的受影响的行数
     * @throws SqlRuntimeException sql执行过程中出现错误
     */
    public int executeNonQuery(final String sql, final Map<String, ?> arg) {
        return executeNonQuery(sql, Collections.singletonList(arg));
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
     *         call test.procedure(); //PostgreSQL 13版 存储过程不需要加大括号(非函数)
     *     </pre>
     * </blockquote>
     *
     * @param procedure 存储过程名
     * @param args      参数
     * @return DataRow
     * @throws SqlRuntimeException 存储过程或函数执行过程中出现错误
     */
    public DataRow executeCallStatement(final String procedure, Map<String, Param> args) {
        String sourceSql = procedure;
        boolean hasArgs = args != null && !args.isEmpty();
        if (hasArgs) {
            sourceSql = prepareSql(procedure, Collections.emptyMap());
        }
        log.debug("Procedure:{}", Printer.colorful(sourceSql, Color.YELLOW));
        log.debug("Args:{}", args);

        Pair<String, List<String>> preparedSqlAndArgNames = SqlUtil.getPreparedSql(sourceSql, Collections.emptyMap());
        final String executeSql = preparedSqlAndArgNames.getItem1();
        final List<String> argNames = preparedSqlAndArgNames.getItem2();

        CallableStatement statement = null;
        Connection connection = getConnection();
        try {
            statement = connection.prepareCall(executeSql);
            List<String> outNames = new ArrayList<>();
            if (hasArgs) {
                JdbcUtil.setStoreArgs(statement, args, argNames);
                for (String name : argNames) {
                    ParamMode mode = args.get(name).getParamMode();
                    if (mode == ParamMode.OUT || mode == ParamMode.IN_OUT) {
                        outNames.add(name);
                    }
                }
            }
            statement.execute();
            printSqlConsole(statement);
            if (outNames.size() > 0) {
                Object[] values = new Object[outNames.size()];
                int resultIndex = 0;
                for (int i = 0; i < argNames.size(); i++) {
                    if (args.get(argNames.get(i)).getParamMode() == ParamMode.OUT || args.get(argNames.get(i)).getParamMode() == ParamMode.IN_OUT) {
                        Object result = statement.getObject(i + 1);
                        if (null == result) {
                            values[resultIndex] = null;
                        } else if (result instanceof ResultSet) {
                            List<DataRow> rows = JdbcUtil.createDataRows((ResultSet) result, executeSql, -1);
                            values[resultIndex] = rows;
                            log.debug("boxing a result with type: cursor, convert to ArrayList<DataRow>, get result by name:{} or index:{}!", outNames.get(resultIndex), resultIndex);
                        } else {
                            values[resultIndex] = result;
                            log.debug("boxing a result, get result by name:{} or index:{}!", outNames.get(resultIndex), resultIndex);
                        }
                        resultIndex++;
                    }
                }
                return DataRow.of(outNames.toArray(new String[0]), values);
            }
            return DataRow.empty();
        } catch (SQLException e) {
            JdbcUtil.closeStatement(statement);
            statement = null;
            releaseConnection(connection, getDataSource());
            throw new SqlRuntimeException("execute procedure [" + procedure + "] error:", e);
        } finally {
            JdbcUtil.closeStatement(statement);
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * 打印sql内部执行中的日志打印<br>
     * e.g. postgresql
     * <blockquote>
     * raise notice 'my console.';
     * </blockquote>
     *
     * @param sc sql执行声明对象
     */
    private void printSqlConsole(Statement sc) {
        if (log.isWarnEnabled()) {
            try {
                SQLWarning warning = sc.getWarnings();
                if (warning != null) {
                    String state = warning.getSQLState();
                    sc.getWarnings().forEach(r -> log.warn("[{}] [{}] {}", LocalDateTime.now(), state, r.getMessage()));
                }
            } catch (SQLException e) {
                log.error("get sql warning error: ", e);
            }
        }
    }
}
