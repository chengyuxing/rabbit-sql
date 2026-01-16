package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.sql.exceptions.DataAccessException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.JdbcUtils;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
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
 */
public abstract class JdbcSupport {
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
     * Parse sql to executable prepared sql.
     *
     * @param sql  sql
     * @param args args
     * @return generated sql meta data
     */
    protected abstract SqlGenerator.PreparedSqlMetaData prepareSql(@NotNull String sql, Map<String, ?> args);

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
        for (Map.Entry<String, List<Integer>> e : names.entrySet()) {
            String name = e.getKey();
            Object value = name.contains(".") ? ValueUtils.getDeepValue(args, name) : args.get(name);
            for (Integer i : e.getValue()) {
                doHandleStatementValue(ps, i, value);
            }
        }
    }

    /**
     * Wraps a given throwable into a {@link DataAccessException} with an optional SQL statement.
     *
     * @param sql       the SQL statement that caused the exception, may be null
     * @param throwable the original exception to wrap
     * @return a new {@link DataAccessException} that wraps the provided throwable and optionally includes the SQL statement
     */
    protected @NotNull RuntimeException wrappedDataAccessException(@Nullable String sql, @NotNull Throwable throwable) {
        if (throwable instanceof DataAccessException) {
            return (DataAccessException) throwable;
        }
        return sql == null
                ? new DataAccessException("Data access failed.", throwable)
                : new DataAccessException("Statement: " + sql, throwable);
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
     * @throws DataAccessException sql execute error
     */
    protected DataRow executeAny(@NotNull final String sql, Map<String, ?> args) {
        SqlGenerator.PreparedSqlMetaData smd = prepareSql(sql, args);
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            ps = connection.prepareStatement(smd.getPrepareSql());
            ps.setQueryTimeout(queryTimeout(sql, smd.getArgs()));
            setPreparedSqlArgs(ps, smd.getArgs(), smd.getArgNameIndexMapping());
            ps.execute();
            JdbcUtils.printSqlConsole(ps);
            return JdbcUtils.getResult(ps, smd.getPrepareSql());
        } catch (Exception e) {
            throw wrappedDataAccessException(smd.getPrepareSql(), e);
        } finally {
            JdbcUtils.closeStatement(ps);
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * Lazily execute query based on {@link Stream} support, real execute query when terminal
     * operation called, every Stream query holds a connection, in case connection pool dead
     * do have to close this stream finally, e.g.
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
     * @throws DataAccessException sql execute error
     */
    protected Stream<DataRow> executeQueryStream(@NotNull final String sql, Map<String, ?> args) {
        SqlGenerator.PreparedSqlMetaData smd = prepareSql(sql, args);
        UncheckedCloseable close = null;
        try {
            Connection connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            // if transaction is active connection will not be close when read stream to the end in 'try-with-resource' block
            close = UncheckedCloseable.wrap(() -> releaseConnection(connection, getDataSource()));
            //noinspection SqlSourceToSinkFlow
            PreparedStatement ps = connection.prepareStatement(smd.getPrepareSql());
            close = close.nest(ps);
            ps.setQueryTimeout(queryTimeout(sql, smd.getArgs()));
            setPreparedSqlArgs(ps, smd.getArgs(), smd.getArgNameIndexMapping());
            ResultSet resultSet = ps.executeQuery();
            close = close.nest(resultSet);
            return StreamSupport.stream(new Spliterators.AbstractSpliterator<DataRow>(Long.MAX_VALUE, Spliterator.ORDERED) {
                final String[] names = JdbcUtils.createNames(resultSet, smd.getPrepareSql());

                @Override
                public boolean tryAdvance(Consumer<? super DataRow> action) {
                    try {
                        if (!resultSet.next()) {
                            return false;
                        }
                        action.accept(JdbcUtils.createDataRow(names, resultSet));
                        return true;
                    } catch (SQLException ex) {
                        throw new IllegalStateException(smd.getPrepareSql(), ex);
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
            throw wrappedDataAccessException(smd.getPrepareSql(), ex);
        }
    }

    /**
     * Batch executes not prepared sql ({@code ddl} or {@code dml}).
     *
     * @param sqlList   more than 1 sql
     * @param batchSize batch size
     * @return affected row count
     * @throws DataAccessException execute sql error
     */
    protected int executeBatch(@NotNull final Iterable<String> sqlList, @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        Connection connection = null;
        Statement s = null;
        try {
            connection = getConnection();
            s = connection.createStatement();
            s.setQueryTimeout(queryTimeout(String.join(";", sqlList), null));
            final Stream.Builder<int[]> result = Stream.builder();
            int i = 1;
            for (String sql : sqlList) {
                String parsedSql = prepareSql(sql, Collections.emptyMap()).getSourceSql();
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
            throw wrappedDataAccessException(String.join(";\n", sqlList), e);
        } finally {
            JdbcUtils.closeStatement(s);
            releaseConnection(connection, getDataSource());
        }
    }

    /**
     * Batch execute prepared non-query sql ({@code insert}, {@code update}, {@code delete}).
     *
     * @param sql        named parameter sql
     * @param args       args collection
     * @param eachMapper each object mapping to Map function
     * @param batchSize  batch size
     * @param <T>        arg type
     * @return affected row count
     * @throws DataAccessException execute procedure error
     */
    protected <T> int executeBatchUpdate(@NotNull final String sql,
                                         @NotNull Iterable<T> args,
                                         @NotNull Function<T, ? extends Map<String, ?>> eachMapper,
                                         @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        Map<String, ?> first = eachMapper.apply(args.iterator().next());
        SqlGenerator.PreparedSqlMetaData smd = prepareSql(sql, first);
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            ps = connection.prepareStatement(smd.getPrepareSql());
            ps.setQueryTimeout(queryTimeout(sql, first));
            final Stream.Builder<int[]> result = Stream.builder();
            int i = 1;
            for (T arg : args) {
                setPreparedSqlArgs(ps, eachMapper.apply(arg), smd.getArgNameIndexMapping());
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
        } catch (Exception e) {
            throw wrappedDataAccessException(smd.getPrepareSql(), e);
        } finally {
            JdbcUtils.closeStatement(ps);
            releaseConnection(connection, getDataSource());
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
     * @throws DataAccessException execute sql error
     */
    protected int executeUpdate(@NotNull final String sql, Map<String, ?> args) {
        SqlGenerator.PreparedSqlMetaData smd = prepareSql(sql, args);
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            ps = connection.prepareStatement(smd.getPrepareSql());
            ps.setQueryTimeout(queryTimeout(sql, smd.getArgs()));
            setPreparedSqlArgs(ps, smd.getArgs(), smd.getArgNameIndexMapping());
            return ps.executeUpdate();
        } catch (Exception e) {
            throw wrappedDataAccessException(smd.getPrepareSql(), e);
        } finally {
            JdbcUtils.closeStatement(ps);
            releaseConnection(connection, getDataSource());
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
     * @throws DataAccessException execute procedure error
     */
    protected DataRow executeCallStatement(@NotNull final String procedure, Map<String, Param> args) {
        SqlGenerator.PreparedSqlMetaData smd = prepareSql(procedure, args);
        Connection connection = null;
        CallableStatement cs = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            cs = connection.prepareCall(smd.getPrepareSql());
            cs.setQueryTimeout(queryTimeout(procedure, args));

            List<String> outNames = new ArrayList<>();
            if (!args.isEmpty()) {
                // adapt postgresql
                // out and inout param first
                for (Map.Entry<String, List<Integer>> e : smd.getArgNameIndexMapping().entrySet()) {
                    Param param = args.get(e.getKey());
                    if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                        for (Integer i : e.getValue()) {
                            cs.registerOutParameter(i, param.getType().typeNumber());
                        }
                        outNames.add(e.getKey());
                    }
                }
                // in param next
                for (Map.Entry<String, List<Integer>> e : smd.getArgNameIndexMapping().entrySet()) {
                    Param param = args.get(e.getKey());
                    if (param.getParamMode() == ParamMode.IN || param.getParamMode() == ParamMode.IN_OUT) {
                        for (Integer i : e.getValue()) {
                            doHandleStatementValue(cs, i, param.getValue());
                        }
                    }
                }
            }

            cs.execute();

            JdbcUtils.printSqlConsole(cs);

            if (outNames.isEmpty()) {
                return JdbcUtils.getResult(cs, smd.getPrepareSql());
            }

            Object[] values = new Object[outNames.size()];
            int resultIndex = 0;
            for (Map.Entry<String, List<Integer>> e : smd.getArgNameIndexMapping().entrySet()) {
                if (outNames.contains(e.getKey())) {
                    for (Integer i : e.getValue()) {
                        Object result = cs.getObject(i);
                        if (result == null) {
                            values[resultIndex] = null;
                        } else if (result instanceof ResultSet) {
                            List<DataRow> rows = JdbcUtils.createDataRows((ResultSet) result, "", -1);
                            JdbcUtils.closeResultSet((ResultSet) result);
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
            throw wrappedDataAccessException(smd.getPrepareSql(), e);
        } finally {
            JdbcUtils.closeStatement(cs);
            releaseConnection(connection, getDataSource());
        }
    }
}
