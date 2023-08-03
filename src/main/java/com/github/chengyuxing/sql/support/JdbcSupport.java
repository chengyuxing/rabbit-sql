package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * <h2>jdbc基本操作支持</h2>
 * 提供基本的通用接口，支持流查询、批量更新、ddl、dml、存储过程、函数、过程语句的执行，支持执行传名参数sql<br>
 * 例如：
 * <blockquote>
 * <pre>select * from ... where name = :name or id in (${!idList}) ${cnd};</pre>
 * </blockquote>
 */
public abstract class JdbcSupport extends SqlParser {
    private final static Logger log = LoggerFactory.getLogger(JdbcSupport.class);

    /**
     * 获取数据源
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
     * 执行一句预编译的sql
     *
     * @param sql      sql
     * @param callback 执行声明回调函数
     * @param <T>      结果类型参数
     * @return 任意类型
     * @throws UncheckedSqlException sql执行过程中出现异常
     */
    protected <T> T execute(final String sql, StatementCallback<T> callback) {
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            statement = connection.prepareStatement(sql);
            return callback.doInStatement(statement);
        } catch (SQLException e) {
            try {
                JdbcUtil.closeStatement(statement);
            } catch (SQLException ex) {
                e.addSuppressed(ex);
            }
            statement = null;
            releaseConnection(connection, getDataSource());
            throw new UncheckedSqlException("execute sql:\n[" + sql + "]", e);
        } finally {
            try {
                JdbcUtil.closeStatement(statement);
            } catch (SQLException e) {
                log.error("close statement error:", e);
            }
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
     * @throws UncheckedSqlException sql执行过程中出现错误
     */
    public DataRow execute(final String sql, Map<String, ?> args) {
        Triple<String, List<String>, Map<String, Object>> prepared = prepare(sql, args);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        final Map<String, Object> data = prepared.getItem3();
        try {
            return execute(preparedSql, ps -> {
                JdbcUtil.setSqlArgs(ps, data, argNames);
                boolean isQuery = ps.execute();
                printSqlConsole(ps);
                DataRow result;
                if (isQuery) {
                    ResultSet resultSet = ps.getResultSet();
                    List<DataRow> rows = JdbcUtil.createDataRows(resultSet, preparedSql, -1);
                    JdbcUtil.closeResultSet(resultSet);
                    if (rows.size() == 1) {
                        result = DataRow.fromPair("result", rows.get(0), "type", "QUERY");
                    } else {
                        result = DataRow.fromPair("result", rows, "type", "QUERY");
                    }
                } else {
                    int count = ps.getUpdateCount();
                    result = DataRow.fromPair("result", count, "type", "DD(M)L");
                }
                return result;
            });
        } catch (Exception e) {
            throw new RuntimeException("prepare sql error:\n[" + sql + "]\n" + data, e);
        }
    }

    /**
     * 惰性执行一句查询，只有调用终端操作和短路操作才会真正开始执行，
     * 使用完请务必关闭流，否则将一直占用连接对象直到连接池耗尽。<br>
     * 使用{@code try-with-resource}进行包裹：
     * <blockquote>
     * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; stream = executeQueryStream(...)) {
     *       stream.limit(10).forEach(System.out::println);
     *         }</pre>
     * </blockquote>
     * 或者使用完手动关闭流：
     * <blockquote>
     * <pre>
     *     {@link Stream}&lt;{@link DataRow}&gt; stream = executeQueryStream(...);
     *     ...
     *     stream.close();
     *     </pre>
     * </blockquote>
     *
     * @param sql  e.g. <code>select * from test.user where id = :id</code>
     * @param args 参数 （占位符名字，参数对象）
     * @return Stream数据流
     * @throws UncheckedSqlException sql执行过程中出现错误或读取结果集是出现错误.
     */
    public Stream<DataRow> executeQueryStream(final String sql, Map<String, ?> args) {
        Triple<String, List<String>, Map<String, Object>> prepared = prepare(sql, args);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        final Map<String, Object> data = prepared.getItem3();
        UncheckedCloseable close = null;
        try {
            Connection connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            // if transaction is active connection will not be close when read stream to the end in 'try-with-resource' block
            close = UncheckedCloseable.wrap(() -> releaseConnection(connection, getDataSource()));
            //noinspection SqlSourceToSinkFlow
            PreparedStatement ps = connection.prepareStatement(preparedSql);
            close = close.nest(ps);
            JdbcUtil.setSqlArgs(ps, data, argNames);
            ResultSet resultSet = ps.executeQuery();
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
                        throw new UncheckedSqlException("reading result set of query: [" + preparedSql + "]\nerror: ", ex);
                    }
                }
            }, false).onClose(close);
        } catch (Exception ex) {
            if (close != null) {
                try {
                    close.close();
                } catch (Exception e) {
                    ex.addSuppressed(e);
                }
            }
            throw new RuntimeException("\nstreaming query error: \n[" + sql + "]\n[" + preparedSql + "]\n" + data, ex);
        }
    }

    /**
     * 批量执行非查询(ddl,dml)语句（非预编译）
     *
     * @param sqls      一组sql
     * @param batchSize 批量执行的数据大小
     * @return 每条sql的执行结果
     * @throws UncheckedSqlException         执行批量操作时发生错误
     * @throws UnsupportedOperationException 数据库或驱动版本不支持批量操作
     * @throws IllegalArgumentException      如果执行的sql条数少1条
     */
    public int executeBatch(final List<String> sqls, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must greater than 1.");
        }
        if (sqls.isEmpty()) {
            return 0;
        }
        Statement s = null;
        Connection connection = getConnection();
        try {
            s = connection.createStatement();
            final Map<String, ?> empty = Collections.emptyMap();
            final Stream.Builder<int[]> result = Stream.builder();
            int i = 1;
            for (String sql : sqls) {
                if (StringUtil.isEmpty(sql)) {
                    continue;
                }
                //noinspection SqlSourceToSinkFlow
                s.addBatch(parseSql(sql, empty).getItem1());
                if (i % batchSize == 0) {
                    result.add(s.executeBatch());
                    s.clearBatch();
                }
                i++;
            }
            result.add(s.executeBatch());
            s.clearBatch();
            return result.build().flatMapToInt(IntStream::of).sum();
        } catch (SQLException e) {
            try {
                JdbcUtil.closeStatement(s);
            } catch (SQLException ex) {
                e.addSuppressed(ex);
            }
            s = null;
            releaseConnection(connection, getDataSource());
            throw new UncheckedSqlException("execute batch error: ", e);
        } finally {
            try {
                JdbcUtil.closeStatement(s);
            } catch (SQLException e) {
                log.error("close statement error: ", e);
            }
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * 批量执行非查询语句
     *
     * @param sql  sql
     * @param args 数据
     * @return 受影响的行数
     */
    public int executeBatchUpdate(final String sql, Collection<? extends Map<String, ?>> args, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must greater than 1.");
        }
        Map<String, ?> first = args.iterator().next();
        Triple<String, List<String>, Map<String, Object>> prepared = prepare(sql, first);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        try {
            return execute(preparedSql, ps -> {
                final Stream.Builder<int[]> result = Stream.builder();
                int i = 1;
                for (Map<String, ?> item : args) {
                    JdbcUtil.setSqlArgs(ps, item, argNames);
                    ps.addBatch();
                    if (i % batchSize == 0) {
                        result.add(ps.executeBatch());
                        ps.clearBatch();
                    }
                    i++;
                }
                result.add(ps.executeBatch());
                ps.clearBatch();
                return result.build().flatMapToInt(IntStream::of).sum();
            });
        } catch (Exception e) {
            throw new RuntimeException("prepare sql error:\n[" + sql + "]\n", e);
        }
    }

    /**
     * 执行非查询语句 (insert，update，delete)<br>
     * e.g.
     * <blockquote>
     * <pre>insert into table (a,b,c) values (:v1,:v2,:v3)</pre>
     * <pre>{v1:'a',v2:'b',v3:'c'}</pre>
     * </blockquote>
     *
     * @param sql  sql
     * @param args 一组数据
     * @return 受影响的行数
     */
    public int executeUpdate(final String sql, Map<String, ?> args) {
        Triple<String, List<String>, Map<String, Object>> prepared = prepare(sql, args);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        final Map<String, Object> data = prepared.getItem3();
        try {
            return execute(preparedSql, sc -> {
                if (data.isEmpty()) {
                    return sc.executeUpdate();
                }
                JdbcUtil.setSqlArgs(sc, data, argNames);
                return sc.executeUpdate();
            });
        } catch (Exception e) {
            throw new RuntimeException("prepare sql error:\n[" + sql + "]\n" + data, e);
        }
    }

    /**
     * 执行存储过程或函数<br>
     * 所有出参结果都放入到{@link DataRow}中，可通过命名参数名来取得。<br>
     * 语句形如原生jdbc，只是将?号改为命名参数（:参数名）：
     * <blockquote>
     * <pre>
     *         {call test.func1(:arg1, :arg2, :result1, :result2)};
     *         {call test.func2(:result::refcursor)}; //PostgreSQL
     *         {:result = call test.func3()};
     *         call test.procedure(); //PostgreSQL 13版+ 存储过程不需要加大括号(非函数)
     *     </pre>
     * </blockquote>
     *
     * @param procedure 存储过程名
     * @param args      参数
     * @return DataRow
     * @throws UncheckedSqlException 存储过程或函数执行过程中出现错误
     */
    public DataRow executeCallStatement(final String procedure, Map<String, Param> args) {
        Pair<String, List<String>> preparedSqlAndArgNames = prepare(procedure, args);
        final String executeSql = preparedSqlAndArgNames.getItem1();
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        CallableStatement statement = null;
        Connection connection = getConnection();
        try {
            //noinspection SqlSourceToSinkFlow
            statement = connection.prepareCall(executeSql);
            List<String> outNames = new ArrayList<>();
            if (!args.isEmpty()) {
                JdbcUtil.setStoreArgs(statement, args, argNames);
                for (String name : argNames) {
                    if (args.containsKey(name)) {
                        ParamMode mode = args.get(name).getParamMode();
                        if (mode == ParamMode.OUT || mode == ParamMode.IN_OUT) {
                            outNames.add(name);
                        }
                    }
                }
            }
            statement.execute();
            printSqlConsole(statement);
            if (!outNames.isEmpty()) {
                Object[] values = new Object[outNames.size()];
                int resultIndex = 0;
                for (int i = 0; i < argNames.size(); i++) {
                    String name = argNames.get(i);
                    if (outNames.contains(name)) {
                        Object result = statement.getObject(i + 1);
                        if (null == result) {
                            values[resultIndex] = null;
                        } else if (result instanceof ResultSet) {
                            List<DataRow> rows = JdbcUtil.createDataRows((ResultSet) result, executeSql, -1);
                            JdbcUtil.closeResultSet((ResultSet) result);
                            values[resultIndex] = rows;
                            log.debug("boxing a result with type: cursor, convert to ArrayList<DataRow>, get result by name:{}!", outNames.get(resultIndex));
                        } else {
                            values[resultIndex] = result;
                            log.debug("boxing a result, get result by name:{}!", outNames.get(resultIndex));
                        }
                        resultIndex++;
                    }
                }
                return DataRow.of(outNames.toArray(new String[0]), values);
            }
            return new DataRow(0);
        } catch (SQLException e) {
            try {
                JdbcUtil.closeStatement(statement);
            } catch (SQLException ex) {
                e.addSuppressed(ex);
            }
            statement = null;
            releaseConnection(connection, getDataSource());
            throw new UncheckedSqlException("execute procedure error: \n[" + procedure + "]\n" + args, e);
        } finally {
            try {
                JdbcUtil.closeStatement(statement);
            } catch (SQLException e) {
                log.error("close statement error: ", e);
            }
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * 打印sql内部执行中的日志打印<br>
     * 例如 postgresql：
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
