package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.exceptions.SqlRuntimeException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.JdbcUtil;
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
            Object value = name.contains(".") ? ObjectUtil.getDeepValue(args, name) : args.get(name);
            for (Integer i : e.getValue()) {
                doHandleStatementValue(ps, i, value);
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
     * @throws SqlRuntimeException sql execute error
     */
    protected DataRow executeAny(@NotNull final String sql, Map<String, ?> args) {
        SqlGenerator.PreparedSqlMetaData sqlMetaData = prepareSql(sql, args);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getPrepareSql();
        final Map<String, ?> myArgs = sqlMetaData.getArgs();
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            ps = connection.prepareStatement(preparedSql);
            ps.setQueryTimeout(queryTimeout(sql, myArgs));
            setPreparedSqlArgs(ps, myArgs, argNames);
            ps.execute();
            JdbcUtil.printSqlConsole(ps);
            return JdbcUtil.getResult(ps, preparedSql);
        } catch (Exception e) {
            throw new SqlRuntimeException("SQL: " + sql + "\nArgs: " + myArgs, e);
        } finally {
            JdbcUtil.closeStatement(ps);
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
     * @throws UncheckedSqlException sql execute error
     */
    protected Stream<DataRow> executeQueryStream(@NotNull final String sql, Map<String, ?> args) {
        SqlGenerator.PreparedSqlMetaData sqlMetaData = prepareSql(sql, args);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getPrepareSql();
        final Map<String, ?> myArgs = sqlMetaData.getArgs();
        UncheckedCloseable close = null;
        try {
            Connection connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            // if transaction is active connection will not be close when read stream to the end in 'try-with-resource' block
            close = UncheckedCloseable.wrap(() -> releaseConnection(connection, getDataSource()));
            //noinspection SqlSourceToSinkFlow
            PreparedStatement ps = connection.prepareStatement(preparedSql);
            close = close.nest(ps);
            ps.setQueryTimeout(queryTimeout(sql, myArgs));
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
                        throw new UncheckedSqlException("Reading result set of query error", ex);
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
            throw new SqlRuntimeException("SQL: " + sql + "\nArgs: " + myArgs, ex);
        }
    }

    /**
     * Batch executes not prepared sql ({@code ddl} or {@code dml}).
     *
     * @param sqlList   more than 1 sql
     * @param batchSize batch size
     * @return affected row count
     * @throws UncheckedSqlException    execute sql error
     * @throws IllegalArgumentException if sql count less than 1
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
            throw new SqlRuntimeException("SQLs: " + String.join(";\n"), e);
        } finally {
            JdbcUtil.closeStatement(s);
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
     */
    protected <T> int executeBatchUpdate(@NotNull final String sql,
                                         @NotNull Iterable<T> args,
                                         @NotNull Function<T, ? extends Map<String, ?>> eachMapper,
                                         @Range(from = 1, to = Integer.MAX_VALUE) int batchSize) {
        Map<String, ?> first = eachMapper.apply(args.iterator().next());
        SqlGenerator.PreparedSqlMetaData sqlMetaData = prepareSql(sql, first);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getPrepareSql();
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            ps = connection.prepareStatement(preparedSql);
            ps.setQueryTimeout(queryTimeout(sql, first));
            final Stream.Builder<int[]> result = Stream.builder();
            int i = 1;
            for (T arg : args) {
                setPreparedSqlArgs(ps, eachMapper.apply(arg), argNames);
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
            StringJoiner argSb = new StringJoiner(",\n");
            int i = 0;
            for (T arg : args) {
                if (i++ >= 10) {
                    argSb.add("......");
                    break;
                }
                argSb.add(eachMapper.apply(arg).toString());
            }
            throw new SqlRuntimeException("SQL: " + sql + "\nArgs: " + argSb, e);
        } finally {
            JdbcUtil.closeStatement(ps);
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
     */
    protected int executeUpdate(@NotNull final String sql, Map<String, ?> args) {
        SqlGenerator.PreparedSqlMetaData sqlMetaData = prepareSql(sql, args);
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        final String preparedSql = sqlMetaData.getPrepareSql();
        final Map<String, ?> myArgs = sqlMetaData.getArgs();
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            ps = connection.prepareStatement(preparedSql);
            ps.setQueryTimeout(queryTimeout(sql, myArgs));
            setPreparedSqlArgs(ps, myArgs, argNames);
            return ps.executeUpdate();
        } catch (Exception e) {
            throw new SqlRuntimeException("SQL: " + sql + "\nArgs: " + myArgs, e);
        } finally {
            JdbcUtil.closeStatement(ps);
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
     * @throws UncheckedSqlException execute procedure error
     */
    protected DataRow executeCallStatement(@NotNull final String procedure, Map<String, Param> args) {
        SqlGenerator.PreparedSqlMetaData sqlMetaData = prepareSql(procedure, args);
        final String sql = sqlMetaData.getPrepareSql();
        final Map<String, List<Integer>> argNames = sqlMetaData.getArgNameIndexMapping();
        Connection connection = null;
        CallableStatement cs = null;
        try {
            connection = getConnection();
            //noinspection SqlSourceToSinkFlow
            cs = connection.prepareCall(sql);
            cs.setQueryTimeout(queryTimeout(procedure, args));

            List<String> outNames = new ArrayList<>();
            if (!args.isEmpty()) {
                // adapt postgresql
                // out and inout param first
                for (Map.Entry<String, List<Integer>> e : argNames.entrySet()) {
                    Param param = args.get(e.getKey());
                    if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                        for (Integer i : e.getValue()) {
                            cs.registerOutParameter(i, param.getType().typeNumber());
                        }
                        outNames.add(e.getKey());
                    }
                }
                // in param next
                for (Map.Entry<String, List<Integer>> e : argNames.entrySet()) {
                    Param param = args.get(e.getKey());
                    if (param.getParamMode() == ParamMode.IN || param.getParamMode() == ParamMode.IN_OUT) {
                        for (Integer i : e.getValue()) {
                            doHandleStatementValue(cs, i, param.getValue());
                        }
                    }
                }
            }

            cs.execute();

            JdbcUtil.printSqlConsole(cs);

            if (outNames.isEmpty()) {
                return JdbcUtil.getResult(cs, sql);
            }

            Object[] values = new Object[outNames.size()];
            int resultIndex = 0;
            for (Map.Entry<String, List<Integer>> e : argNames.entrySet()) {
                if (outNames.contains(e.getKey())) {
                    for (Integer i : e.getValue()) {
                        Object result = cs.getObject(i);
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
            throw new SqlRuntimeException("PROCEDURE: " + procedure + "\nArgs: " + args, e);
        } finally {
            JdbcUtil.closeStatement(cs);
            releaseConnection(connection, getDataSource());
        }
    }
}
