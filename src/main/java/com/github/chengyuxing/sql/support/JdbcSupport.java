package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlHighlighter;
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
 * Provide basic support: {@link  Stream stream query}, {@code ddl}, {@code dml}, {@code store procedure/function}, {@code plsql}.<br>
 * <p>e.g. sql statement:</p>
 * <blockquote>
 * <pre>select * from ...
 * where name = :name
 * or id in (${!idList}) ${cnd};</pre>
 * </blockquote>
 *
 * @see com.github.chengyuxing.sql.types.Variable
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
     * Sql watcher
     *
     * @param sql       sql
     * @param args      args
     * @param startTime connection request start time
     * @param endTime   finish execute time
     */
    protected void watchSql(String sql, Object args, long startTime, long endTime, Throwable throwable) {

    }

    /**
     * jdbc execute sql timeout.
     *
     * @return time out (seconds)
     * @see Statement#setQueryTimeout(int)
     */
    protected int queryTimeout() {
        return 0;
    }

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
            throw new UncheckedSqlException("execute sql:\n" + sql, e);
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
     * @param names ordered arg names
     * @throws SQLException if connection states error
     */
    protected void setPreparedSqlArgs(PreparedStatement ps, Map<String, ?> args, Map<String, List<Integer>> names) throws SQLException {
        for (Map.Entry<String, List<Integer>> e : names.entrySet()) {
            String name = e.getKey();
            Object value = name.contains(".") ? ObjectUtil.getDeepValue(args, name) : args.get(name);
            for (Integer i : e.getValue()) {
                doHandleStatementValue(ps, i, value);
            }
        }
    }

    /**
     * Set callable statement args.
     *
     * @param cs    store procedure/function statement object
     * @param args  args
     * @param names ordered arg names
     * @throws SQLException if connection states error
     */
    protected void setPreparedStoreArgs(CallableStatement cs, Map<String, Param> args, Map<String, List<Integer>> names) throws SQLException {
        if (args != null && !args.isEmpty()) {
            // adapt postgresql
            // out and inout param first
            for (Map.Entry<String, List<Integer>> e : names.entrySet()) {
                Param param = args.get(e.getKey());
                if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                    for (Integer i : e.getValue()) {
                        cs.registerOutParameter(i, param.getType().typeNumber());
                    }
                }
            }
            // in param next
            for (Map.Entry<String, List<Integer>> e : names.entrySet()) {
                Param param = args.get(e.getKey());
                for (Integer i : e.getValue()) {
                    if (param.getParamMode() == ParamMode.IN || param.getParamMode() == ParamMode.IN_OUT) {
                        doHandleStatementValue(cs, i, param.getValue());
                    }
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
        long startTime = System.currentTimeMillis();
        SqlGenerator.GeneratedSqlMetaData sqlMetaData = prepare(sql, args);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getResultSql();
        final Map<String, ?> myArgs = sqlMetaData.getArgs();
        Throwable reason = null;
        try {
            debugSql(sqlMetaData.getNamedParamSql(), Collections.singletonList(myArgs));
            return execute(preparedSql, ps -> {
                ps.setQueryTimeout(queryTimeout());
                setPreparedSqlArgs(ps, myArgs, argNames);
                boolean isQuery = ps.execute();
                printSqlConsole(ps);
                if (isQuery) {
                    ResultSet resultSet = ps.getResultSet();
                    List<DataRow> rows = JdbcUtil.createDataRows(resultSet, preparedSql, -1);
                    JdbcUtil.closeResultSet(resultSet);
                    return DataRow.of("result", rows, "type", "QUERY");
                }
                int count = ps.getUpdateCount();
                return DataRow.of("result", count, "type", "DD(M)L");
            });
        } catch (Exception e) {
            reason = e;
            throw new RuntimeException("prepare sql error:\n" + sql + "\n" + myArgs, e);
        } finally {
            watchSql(sql, myArgs, startTime, System.currentTimeMillis(), reason);
        }
    }

    /**
     * Lazy execute query based on {@link Stream} support, real execute query when terminal
     * operation called, every Stream query hold a connection, in case connection pool dead
     * do must have to close this stream finally, e.g.
     * <p>Auto close by {@code try-with-resource}:</p>
     * <blockquote>
     * <pre>
     * try ({@link Stream}&lt;{@link DataRow}&gt; stream = executeQueryStream(...)) {
     *      stream.limit(10).forEach(System.out::println);
     * }</pre>
     * </blockquote>
     * <p>Manual close by call {@link Stream#close()}:</p>
     * <blockquote>
     * <pre>
     * {@link Stream}&lt;{@link DataRow}&gt; stream = executeQueryStream(...);
     * ...
     * stream.close();</pre>
     * </blockquote>
     *
     * @param sql  named parameter sql, e.g. <code>select * from test.user where id = :id</code>
     * @param args args
     * @return Stream query result
     * @throws UncheckedSqlException sql execute error
     */
    public Stream<DataRow> executeQueryStream(final String sql, Map<String, ?> args) {
        long startTime = System.currentTimeMillis();
        SqlGenerator.GeneratedSqlMetaData sqlMetaData = prepare(sql, args);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getResultSql();
        final Map<String, ?> myArgs = sqlMetaData.getArgs();
        UncheckedCloseable close = null;
        Throwable reason = null;
        try {
            Connection connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            // if transaction is active connection will not be close when read stream to the end in 'try-with-resource' block
            close = UncheckedCloseable.wrap(() -> releaseConnection(connection, getDataSource()));
            debugSql(sqlMetaData.getNamedParamSql(), Collections.singletonList(myArgs));
            //noinspection SqlSourceToSinkFlow
            PreparedStatement ps = connection.prepareStatement(preparedSql);
            ps.setQueryTimeout(queryTimeout());
            close = close.nest(ps);
            setPreparedSqlArgs(ps, myArgs, argNames);
            ResultSet resultSet = ps.executeQuery();
            close = close.nest(resultSet);
            return StreamSupport.stream(new Spliterators.AbstractSpliterator<DataRow>(Long.MAX_VALUE, Spliterator.ORDERED) {
                final String[] names = JdbcUtil.createNames(resultSet, preparedSql);

                @Override
                public boolean tryAdvance(Consumer<? super DataRow> action) {
                    try {
                        if (!resultSet.next()) {
                            return false;
                        }
                        action.accept(JdbcUtil.createDataRow(names, resultSet));
                        return true;
                    } catch (SQLException ex) {
                        throw new UncheckedSqlException("reading result set of query:\n" + preparedSql + "\nerror.", ex);
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
            reason = ex;
            throw new RuntimeException("streaming query error:\n" + preparedSql + "\n" + myArgs, ex);
        } finally {
            watchSql(sql, myArgs, startTime, System.currentTimeMillis(), reason);
        }
    }

    /**
     * Batch execute not prepared sql ({@code ddl} or {@code dml}).
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
        long startTime = System.currentTimeMillis();
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
     * Batch execute prepared non-query sql ({@code insert}, {@code update}, {@code delete}).
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
        SqlGenerator.GeneratedSqlMetaData sqlMetaData = prepare(sql, first);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getResultSql();
        try {
            debugSql(sqlMetaData.getNamedParamSql(), args);
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
            throw new RuntimeException("prepare sql error:\n" + sql, e);
        }
    }

    /**
     * Execute prepared non-query sql ({@code insert}, {@code update}, {@code delete})
     * <p>e.g. insert statement:</p>
     * <blockquote>
     * <pre>insert into table (a,b,c) values (:v1,:v2,:v3)</pre>
     * </blockquote>
     * <p>args:</p>
     * <blockquote>
     * <pre>{v1:'a',v2:'b',v3:'c'}</pre>
     * </blockquote>
     *
     * @param sql  named parameter sql
     * @param args args
     * @return affect row count
     */
    public int executeUpdate(final String sql, Map<String, ?> args) {
        long startTime = System.currentTimeMillis();
        SqlGenerator.GeneratedSqlMetaData sqlMetaData = prepare(sql, args);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getResultSql();
        final Map<String, ?> myArgs = sqlMetaData.getArgs();
        Throwable reason = null;
        try {
            debugSql(sqlMetaData.getNamedParamSql(), Collections.singletonList(myArgs));
            return execute(preparedSql, sc -> {
                sc.setQueryTimeout(queryTimeout());
                if (myArgs.isEmpty()) {
                    return sc.executeUpdate();
                }
                setPreparedSqlArgs(sc, myArgs, argNames);
                return sc.executeUpdate();
            });
        } catch (Exception e) {
            reason = e;
            throw new RuntimeException("prepare sql error:\n" + sql + "\n" + myArgs, e);
        } finally {
            watchSql(sql, myArgs, startTime, System.currentTimeMillis(), reason);
        }
    }

    /**
     * Execute store {@code procedure} or {@code function}.
     * <blockquote>
     * <pre>
     * { call func1(:in1, :in2, :out1, :out2) }
     * { call func2(:out::refcursor) } //postgresql
     * { :out = call func3() }
     * { call func_returns_table() } //postgresql
     * call procedure() //postgresql v13+
     * </pre>
     * </blockquote>
     * <p>2 ways to get result:</p>
     * <ul>
     *     <li>zero OUT parameters: {@link DataRow#getFirst(Object...) getFirst()} or {@link DataRow#getFirstAs(Object...) getFirstAs()}</li>
     *     <li>by OUT parameter name: {@link DataRow#getAs(String, Object...) getAs(String)} or {@link DataRow#get(Object) get(String)}</li>
     * </ul>
     *
     * @param procedure procedure
     * @param args      args
     * @return DataRow
     * @throws UncheckedSqlException execute procedure error
     */
    public DataRow executeCallStatement(final String procedure, Map<String, Param> args) {
        long startTime = System.currentTimeMillis();
        SqlGenerator.GeneratedSqlMetaData sqlMetaData = prepare(procedure, args);
        final String executeSql = sqlMetaData.getResultSql();
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        Connection connection = getConnection();
        CallableStatement statement = null;
        Throwable reason = null;
        try {
            debugSql(sqlMetaData.getNamedParamSql(), Collections.singletonList(args));
            //noinspection SqlSourceToSinkFlow
            statement = connection.prepareCall(executeSql);
            statement.setQueryTimeout(queryTimeout());
            List<String> outNames = new ArrayList<>();
            if (!args.isEmpty()) {
                setPreparedStoreArgs(statement, args, argNames);
                for (String name : argNames.keySet()) {
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
            for (Map.Entry<String, List<Integer>> e : argNames.entrySet()) {
                if (outNames.contains(e.getKey())) {
                    for (Integer i : e.getValue()) {
                        Object result = statement.getObject(i);
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
            }
            return DataRow.of(outNames.toArray(new String[0]), values);
        } catch (SQLException e) {
            try {
                JdbcUtil.closeStatement(statement);
            } catch (SQLException ex) {
                e.addSuppressed(ex);
            }
            statement = null;
            reason = e;
            releaseConnection(connection, getDataSource());
            throw new UncheckedSqlException("execute procedure error:\n" + procedure + "\n" + args, e);
        } finally {
            try {
                JdbcUtil.closeStatement(statement);
            } catch (SQLException e) {
                log.error("close statement error.", e);
            }
            releaseConnection(connection, getDataSource());
            watchSql(procedure, args, startTime, System.currentTimeMillis(), reason);
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
            log.debug("SQL: {}", SqlHighlighter.highlightIfAnsiCapable(sql));
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
     * <blockquote><pre>
     * raise notice 'my console.';</pre>
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
                    warning.forEach(r -> log.warn("[{}] [{}] {}", LocalDateTime.now(), state, r.getMessage()));
                }
            } catch (SQLException e) {
                log.error("get sql warning error.", e);
            }
        }
    }
}
