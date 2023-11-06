package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.common.tuple.Quadruple;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.github.chengyuxing.sql.utils.SqlUtil;
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
 * <h2>JDBC support</h2>
 * Provide basic support: stream query, ddl, dml, store procedure/function, plsql.<br>
 *  e.g.
 * <blockquote>
 * <pre>select * from ... where name = :name or id in (${!idList}) ${cnd};</pre>
 * </blockquote>
 */
public abstract class JdbcSupport extends SqlParser {
    private final static Logger log = LoggerFactory.getLogger(JdbcSupport.class);

    /**
     * Get datasource.
     *
     * @return datasource
     */
    protected abstract DataSource getDataSource();

    /**
     * Get connection.
     *
     * @return connection
     */
    protected abstract Connection getConnection();

    /**
     * Release connection when execute finished.
     *
     * @param connection connection
     * @param dataSource datasource
     */
    protected abstract void releaseConnection(Connection connection, DataSource dataSource);

    /**
     * Handle prepared statement value.
     *
     * @param ps    PreparedStatement
     * @param index parameter index
     * @param value parameter value
     * @throws SQLException ex
     */
    protected abstract void doHandleStatementValue(PreparedStatement ps, int index, Object value) throws SQLException;

    /**
     * Execute any prepared sql.
     *
     * @param sql      sql
     * @param callback statement callback
     * @param <T>      result type
     * @return any result
     * @throws UncheckedSqlException if connection states error
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
            throw new UncheckedSqlException("execute sql: [" + sql + "]", e);
        } finally {
            try {
                JdbcUtil.closeStatement(statement);
            } catch (SQLException e) {
                log.error("close statement error.", e);
            }
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * Set prepared sql statement args.
     *
     * @param ps    sql statement object
     * @param args  args
     * @param names sorted arg names
     * @throws SQLException if connection states error
     */
    protected void setPreparedSqlArgs(PreparedStatement ps, Map<String, ?> args, List<String> names) throws SQLException {
        for (int i = 0; i < names.size(); i++) {
            int index = i + 1;
            String name = names.get(i);
            Object value = name.contains(".") ? ObjectUtil.getDeepValue(args, name) : args.get(name);
            doHandleStatementValue(ps, index, value);
        }
    }

    /**
     * Set callable statement args.
     *
     * @param cs    store procedure/function statement object
     * @param args  args
     * @param names sorted arg names
     * @throws SQLException if connection states error
     */
    protected void setPreparedStoreArgs(CallableStatement cs, Map<String, Param> args, List<String> names) throws SQLException {
        if (args != null && !args.isEmpty()) {
            // adapt postgresql
            // out and inout param first
            for (int i = 0; i < names.size(); i++) {
                Param param = args.get(names.get(i));
                if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                    cs.registerOutParameter(i + 1, param.getType().typeNumber());
                }
            }
            // in param next
            for (int i = 0; i < names.size(); i++) {
                Param param = args.get(names.get(i));
                if (param.getParamMode() == ParamMode.IN || param.getParamMode() == ParamMode.IN_OUT) {
                    doHandleStatementValue(cs, i + 1, param.getValue());
                }
            }
        }
    }

    /**
     * Execute query, ddl, dml or plsql statement.<br>
     * Execute result:<br>
     * <ul>
     *     <li>result: {@link DataRow#getFirst(Object...) getFirst()} or {@link DataRow#get(Object) get("result")}</li>
     *     <li>type: {@link DataRow#getString(int, String...) getString(1)} 或 {@link DataRow#get(Object) get("type")}</li>
     * </ul>
     *
     * @param sql  named parameter sql
     * @param args args
     * @return Query: List{@code <DataRow>}, DML: affected row count, DDL: 0
     * @throws UncheckedSqlException sql execute error
     */
    public DataRow execute(final String sql, Map<String, ?> args) {
        Quadruple<String, List<String>, Map<String, Object>, String> prepared = prepare(sql, args);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        final Map<String, Object> data = prepared.getItem3();
        try {
            debugSql(prepared.getItem4(), Collections.singletonList(data));
            return execute(preparedSql, ps -> {
                setPreparedSqlArgs(ps, data, argNames);
                boolean isQuery = ps.execute();
                printSqlConsole(ps);
                DataRow result;
                if (isQuery) {
                    ResultSet resultSet = ps.getResultSet();
                    List<DataRow> rows = JdbcUtil.createDataRows(resultSet, preparedSql, -1);
                    JdbcUtil.closeResultSet(resultSet);
                    result = DataRow.of("result", rows, "type", "QUERY");
                } else {
                    int count = ps.getUpdateCount();
                    result = DataRow.of("result", count, "type", "DD(M)L");
                }
                return result;
            });
        } catch (Exception e) {
            throw new RuntimeException("prepare sql error:\n[" + sql + "]\n" + data, e);
        }
    }

    /**
     * Lazy execute query based on {@link Stream} support, real execute query when terminal
     * operation called, every Stream query hold a connection, in case connection pool dead
     * do must have to close this stream finally.<br>
     *  e.g.<br>
     * Auto close by {@code try-with-resource}:
     * <blockquote>
     * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; stream = executeQueryStream(...)) {
     *       stream.limit(10).forEach(System.out::println);
     *         }</pre>
     * </blockquote>
     * Manual close by call {@link Stream#close()}:
     * <blockquote>
     * <pre>
     *     {@link Stream}&lt;{@link DataRow}&gt; stream = executeQueryStream(...);
     *     ...
     *     stream.close();
     *     </pre>
     * </blockquote>
     *
     * @param sql  named parameter sql, e.g. <code>select * from test.user where id = :id</code>
     * @param args args
     * @return Stream query result
     * @throws UncheckedSqlException sql execute error
     */
    public Stream<DataRow> executeQueryStream(final String sql, Map<String, ?> args) {
        Quadruple<String, List<String>, Map<String, Object>, String> prepared = prepare(sql, args);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        final Map<String, Object> data = prepared.getItem3();
        UncheckedCloseable close = null;
        try {
            Connection connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            // if transaction is active connection will not be close when read stream to the end in 'try-with-resource' block
            close = UncheckedCloseable.wrap(() -> releaseConnection(connection, getDataSource()));
            debugSql(prepared.getItem4(), Collections.singletonList(data));
            //noinspection SqlSourceToSinkFlow
            PreparedStatement ps = connection.prepareStatement(preparedSql);
            close = close.nest(ps);
            setPreparedSqlArgs(ps, data, argNames);
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
     * Batch execute not prepared sql (ddl or dml).
     *
     * @param sqls      more than 1 sql
     * @param batchSize batch size
     * @return affected row count
     * @throws UncheckedSqlException    execute sql error
     * @throws IllegalArgumentException if sqls count less than 1
     */
    public int executeBatch(final List<String> sqls, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must greater than 0.");
        }
        if (sqls.isEmpty()) {
            return 0;
        }
        Statement s = null;
        Connection connection = getConnection();
        try {
            s = connection.createStatement();
            final Stream.Builder<int[]> result = Stream.builder();
            int i = 1;
            for (String sql : sqls) {
                if (StringUtil.isEmpty(sql)) {
                    continue;
                }
                String parsedSql = parseSql(sql, Collections.emptyMap()).getItem1();
                debugSql(parsedSql, Collections.emptyList());
                //noinspection SqlSourceToSinkFlow
                s.addBatch(parsedSql);
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
            throw new UncheckedSqlException("execute batch error.", e);
        } finally {
            try {
                JdbcUtil.closeStatement(s);
            } catch (SQLException e) {
                log.error("close statement error.", e);
            }
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * Batch execute prepared non-query sql (insert, update, delete).
     *
     * @param sql       named parameter sql
     * @param args      args collection
     * @param batchSize batch size
     * @return affected row count
     */
    public int executeBatchUpdate(final String sql, Collection<? extends Map<String, ?>> args, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must greater than 0.");
        }
        Map<String, ?> first = args.iterator().next();
        Quadruple<String, List<String>, Map<String, Object>, String> prepared = prepare(sql, first);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        try {
            debugSql(prepared.getItem4(), args);
            return execute(preparedSql, ps -> {
                final Stream.Builder<int[]> result = Stream.builder();
                int i = 1;
                for (Map<String, ?> item : args) {
                    setPreparedSqlArgs(ps, item, argNames);
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
     * Execute prepared non-query sql (insert, update, delete)<br>
     *  e.g.
     * <blockquote>
     * <pre>insert into table (a,b,c) values (:v1,:v2,:v3)</pre>
     * <pre>{v1:'a',v2:'b',v3:'c'}</pre>
     * </blockquote>
     *
     * @param sql  named parameter sql
     * @param args args
     * @return affect row count
     */
    public int executeUpdate(final String sql, Map<String, ?> args) {
        Quadruple<String, List<String>, Map<String, Object>, String> prepared = prepare(sql, args);
        final List<String> argNames = prepared.getItem2();
        final String preparedSql = prepared.getItem1();
        final Map<String, Object> data = prepared.getItem3();
        try {
            debugSql(prepared.getItem4(), Collections.singletonList(data));
            return execute(preparedSql, sc -> {
                if (data.isEmpty()) {
                    return sc.executeUpdate();
                }
                setPreparedSqlArgs(sc, data, argNames);
                return sc.executeUpdate();
            });
        } catch (Exception e) {
            throw new RuntimeException("prepare sql error:\n[" + sql + "]\n" + data, e);
        }
    }

    /**
     * Execute store procedure/function.<br>
     * <blockquote>
     * <pre>
     * { call func1(:in1, :in2, :out1, :out2) }
     * { call func2(:out::refcursor) } //postgresql
     * { :out = call func3() }
     * { call func_returns_table() } //postgresql
     * call procedure() //postgresql v13+
     * </pre>
     * </blockquote>
     * Get result depends on have OUT parameter or not:
     * <blockquote>
     * <ul>
     *     <li>without OUT parameter: {@link DataRow#getFirst(Object...) getFirst()} or {@link DataRow#getFirstAs(Object...) getFirstAs()}</li>
     *     <li>by OUT parameter name: {@link DataRow#getAs(String, Object...) getAs(String)} or {@link DataRow#get(Object) get(String)}</li>
     * </ul>
     * </blockquote>
     *
     * @param procedure procedure
     * @param args      args
     * @return DataRow
     * @throws UncheckedSqlException execute procedure error
     */
    public DataRow executeCallStatement(final String procedure, Map<String, Param> args) {
        Quadruple<String, List<String>, Map<String, Object>, String> prepared = prepare(procedure, args);
        final String executeSql = prepared.getItem1();
        final List<String> argNames = prepared.getItem2();
        CallableStatement statement = null;
        Connection connection = getConnection();
        try {
            debugSql(prepared.getItem4(), Collections.singletonList(args));
            //noinspection SqlSourceToSinkFlow
            statement = connection.prepareCall(executeSql);
            List<String> outNames = new ArrayList<>();
            if (!args.isEmpty()) {
                setPreparedStoreArgs(statement, args, argNames);
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
            if (outNames.isEmpty()) {
                ResultSet resultSet = statement.getResultSet();
                List<DataRow> dataRows = JdbcUtil.createDataRows(resultSet, "", -1);
                JdbcUtil.closeResultSet(resultSet);
                if (dataRows.isEmpty()) {
                    return DataRow.of();
                }
                return DataRow.of("result", dataRows);
            }
            Object[] values = new Object[outNames.size()];
            int resultIndex = 0;
            for (int i = 0; i < argNames.size(); i++) {
                String name = argNames.get(i);
                if (outNames.contains(name)) {
                    Object result = statement.getObject(i + 1);
                    if (Objects.isNull(result)) {
                        values[resultIndex] = null;
                    } else if (result instanceof ResultSet) {
                        List<DataRow> rows = JdbcUtil.createDataRows((ResultSet) result, "", -1);
                        JdbcUtil.closeResultSet((ResultSet) result);
                        values[resultIndex] = rows;
                    } else {
                        values[resultIndex] = result;
                    }
                    resultIndex++;
                }
            }
            return DataRow.of(outNames.toArray(new String[0]), values);
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
                log.error("close statement error.", e);
            }
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * Debug executed sql and args.
     *
     * @param sql  sql
     * @param args args
     */
    protected void debugSql(String sql, Collection<? extends Map<String, ?>> args) {
        if (log.isDebugEnabled()) {
            log.debug("SQL: {}", SqlUtil.highlightSqlIfConsole(sql));
            for (Map<String, ?> arg : args) {
                StringJoiner sb = new StringJoiner(", ", "{", "}");
                arg.forEach((k, v) -> {
                    if (v == null) {
                        sb.add(k + " -> null");
                    } else {
                        sb.add(k + " -> " + v + "(" + v.getClass().getSimpleName() + ")");
                    }
                });
                log.debug("Args: {}", sb);
            }
        }
    }

    /**
     * Print sql log, e.g postgresql：
     * <blockquote>
     * raise notice 'my console.';
     * </blockquote>
     *
     * @param sc sql statement object
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
                log.error("get sql warning error.", e);
            }
        }
    }
}
