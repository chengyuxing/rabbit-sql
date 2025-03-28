package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.SqlRuntimeException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
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
    protected abstract @NotNull DataSource getDataSource();

    /**
     * Get connection.
     *
     * @return connection
     */
    protected abstract @NotNull Connection getConnection();

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
    protected abstract void doHandleStatementValue(@NotNull PreparedStatement ps,
                                                   @Range(from = 1, to = Integer.MAX_VALUE) int index,
                                                   @Nullable Object value) throws SQLException;

    /**
     * Jdbc execute sql timeout.
     *
     * @param sql  sql
     * @param args args
     * @return time out (seconds)
     * @see Statement#setQueryTimeout(int)
     */
    protected @Range(from = 0, to = Integer.MAX_VALUE) int queryTimeout(String sql, Map<String, ?> args) {
        return 0;
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
        for (var e : names.entrySet()) {
            var name = e.getKey();
            var value = name.contains(".") ? ObjectUtil.getDeepValue(args, name) : args.get(name);
            for (var i : e.getValue()) {
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
            for (var e : names.entrySet()) {
                var param = args.get(e.getKey());
                if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                    for (var i : e.getValue()) {
                        cs.registerOutParameter(i, param.getType().typeNumber());
                    }
                }
            }
            // in param next
            for (var e : names.entrySet()) {
                var param = args.get(e.getKey());
                for (var i : e.getValue()) {
                    if (param.getParamMode() == ParamMode.IN || param.getParamMode() == ParamMode.IN_OUT) {
                        doHandleStatementValue(cs, i, param.getValue());
                    }
                }
            }
        }
    }

    /**
     * Execute any prepared sql.
     *
     * @param sql      sql
     * @param callback statement callback
     * @param <T>      result type
     * @return any result
     * @throws SqlRuntimeException sql execute error
     */
    public <T> T execute(@NotNull final String sql, @NotNull StatementCallback<T> callback) {
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
            throw new SqlRuntimeException(printExceptionMessage(sql, sql, Collections.emptyMap()), e);
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
     * @throws SqlRuntimeException sql execute error
     */
    public DataRow execute(@NotNull final String sql, Map<String, ?> args) {
        final var sqlMetaData = prepare(sql, args);
        final var argNames = sqlMetaData.getArgNameIndexMapping();
        final var preparedSql = sqlMetaData.getResultSql();
        final var myArgs = sqlMetaData.getArgs();
        try {
            debugSql(sql, sqlMetaData.getNamedParamSql(), Collections.singletonList(myArgs));
            return execute(preparedSql, ps -> {
                ps.setQueryTimeout(queryTimeout(sql, myArgs));
                setPreparedSqlArgs(ps, myArgs, argNames);
                ps.execute();
                JdbcUtil.printSqlConsole(ps);
                return JdbcUtil.getResult(ps, preparedSql);
            });
        } catch (Exception e) {
            throw new SqlRuntimeException(printExceptionMessage(sql, preparedSql, myArgs), e);
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
    public Stream<DataRow> executeQueryStream(@NotNull final String sql, Map<String, ?> args) {
        final var sqlMetaData = prepare(sql, args);
        final var argNames = sqlMetaData.getArgNameIndexMapping();
        final var preparedSql = sqlMetaData.getResultSql();
        final var myArgs = sqlMetaData.getArgs();
        UncheckedCloseable close = null;
        try {
            final var connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            // if transaction is active connection will not be close when read stream to the end in 'try-with-resource' block
            close = UncheckedCloseable.wrap(() -> releaseConnection(connection, getDataSource()));
            debugSql(sql, sqlMetaData.getNamedParamSql(), Collections.singletonList(myArgs));
            //noinspection SqlSourceToSinkFlow
            var ps = connection.prepareStatement(preparedSql);
            ps.setQueryTimeout(queryTimeout(sql, myArgs));
            close = close.nest(ps);
            setPreparedSqlArgs(ps, myArgs, argNames);
            var resultSet = ps.executeQuery();
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
                        throw new UncheckedSqlException("reading result set of query error:\n" + preparedSql, ex);
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
            throw new SqlRuntimeException(printExceptionMessage(sql, preparedSql, myArgs), ex);
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
    public int executeBatch(@NotNull final List<String> sqls, @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        if (sqls.isEmpty()) {
            return 0;
        }
        Statement s = null;
        final var connection = getConnection();
        try {
            s = connection.createStatement();
            final Stream.Builder<int[]> result = Stream.builder();
            int i = 1;
            for (var sql : sqls) {
                if (StringUtil.isEmpty(sql)) {
                    continue;
                }
                var parsedSql = parseSql(sql, Collections.emptyMap()).getItem1();
                debugSql(sql, parsedSql, Collections.emptyList());
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
            throw new SqlRuntimeException("execute batch error.", e);
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
    public int executeBatchUpdate(@NotNull final String sql,
                                  @NotNull Collection<? extends Map<String, ?>> args,
                                  @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        final var first = args.iterator().next();
        final var sqlMetaData = prepare(sql, first);
        final var argNames = sqlMetaData.getArgNameIndexMapping();
        final var preparedSql = sqlMetaData.getResultSql();
        try {
            debugSql(sql, sqlMetaData.getNamedParamSql(), args);
            return execute(preparedSql, ps -> {
                final Stream.Builder<int[]> result = Stream.builder();
                int i = 1;
                for (var item : args) {
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
            throw new SqlRuntimeException(printExceptionMessage(sql, preparedSql, args), e);
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
    public int executeUpdate(@NotNull final String sql, Map<String, ?> args) {
        final var sqlMetaData = prepare(sql, args);
        final var argNames = sqlMetaData.getArgNameIndexMapping();
        final var preparedSql = sqlMetaData.getResultSql();
        final var myArgs = sqlMetaData.getArgs();
        try {
            debugSql(sql, sqlMetaData.getNamedParamSql(), Collections.singletonList(myArgs));
            return execute(preparedSql, sc -> {
                sc.setQueryTimeout(queryTimeout(sql, myArgs));
                if (myArgs.isEmpty()) {
                    return sc.executeUpdate();
                }
                setPreparedSqlArgs(sc, myArgs, argNames);
                return sc.executeUpdate();
            });
        } catch (Exception e) {
            throw new SqlRuntimeException(printExceptionMessage(sql, preparedSql, myArgs), e);
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
    public DataRow executeCallStatement(@NotNull final String procedure, Map<String, Param> args) {
        final var sqlMetaData = prepare(procedure, args);
        final var sql = sqlMetaData.getResultSql();
        final var argNames = sqlMetaData.getArgNameIndexMapping();
        final var connection = getConnection();
        CallableStatement cs = null;
        try {
            List<String> outNames = new ArrayList<>();
            if (!args.isEmpty()) {
                for (String name : argNames.keySet()) {
                    if (args.containsKey(name)) {
                        ParamMode mode = args.get(name).getParamMode();
                        if (mode == ParamMode.OUT || mode == ParamMode.IN_OUT) {
                            outNames.add(name);
                        }
                    }
                }
            }

            debugSql(procedure, sqlMetaData.getNamedParamSql(), Collections.singletonList(args));
            //noinspection SqlSourceToSinkFlow
            cs = connection.prepareCall(sql);
            cs.setQueryTimeout(queryTimeout(procedure, args));
            setPreparedStoreArgs(cs, args, argNames);
            cs.execute();
            JdbcUtil.printSqlConsole(cs);

            if (outNames.isEmpty()) {
                return JdbcUtil.getResult(cs, sql);
            }

            var values = new Object[outNames.size()];
            int resultIndex = 0;
            for (var e : argNames.entrySet()) {
                if (outNames.contains(e.getKey())) {
                    for (var i : e.getValue()) {
                        var result = cs.getObject(i);
                        if (Objects.isNull(result)) {
                            values[resultIndex] = null;
                        } else if (result instanceof ResultSet) {
                            ResultSet resultSet = (ResultSet) result;
                            var rows = JdbcUtil.createDataRows(resultSet, "", -1);
                            JdbcUtil.closeResultSet(resultSet);
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
                JdbcUtil.closeStatement(cs);
            } catch (SQLException ex) {
                e.addSuppressed(ex);
            }
            cs = null;
            releaseConnection(connection, getDataSource());
            throw new SqlRuntimeException(printExceptionMessage(procedure, sql, args), e);
        } finally {
            try {
                JdbcUtil.closeStatement(cs);
            } catch (SQLException e) {
                log.error("close statement error.", e);
            }
            releaseConnection(connection, getDataSource());
        }
    }

    protected String printExceptionMessage(String sourceSql, String executeSql, Object args) {
        return executeSql + "\n" + args;
    }

    /**
     * Debug executed sql and args.
     *
     * @param sourceSql  sql name
     * @param executeSql sql
     * @param args       args
     */
    protected void debugSql(String sourceSql, String executeSql, Collection<? extends Map<String, ?>> args) {
        log.debug("SQL: {}, Args: {}", executeSql, args);
    }
}
