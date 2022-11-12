package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.UncheckedCloseable;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.CollectionUtil;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * <h2>jdbc基本操作支持</h2>
 * <p>:name (jdbc标准的传名参数写法，参数将被预编译安全处理)</p>
 * <p>${...} (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)</p>
 * 字符串模版参数名两种格式：
 * <blockquote>
 *     <ul>
 *         <li>${part} 如果类型是装箱类型数组(String[], Integer[]...)或集合(Set, List...)，则先展开（逗号分割），再进行sql片段的替换；</li>
 *         <li>${:part} 名字前多了前缀符号(:)，如果类型是装箱类型数组(String[], Integer[]...)或集合(Set, List...)，则先展开（逗号分隔），并做一定的字符串安全处理，再进行sql片段的替换。</li>
 *     </ul>
 * </blockquote>
 * <p>小提示：PostgreSQL中，带有问号的操作符(?,?|,?&amp;,@?)可以使用双问号(??,??|,??&amp;,@??)解决预编译sql参数未设定的报错，或者直接使用函数</p>
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
 *
 * @see SqlTranslator
 */
public abstract class JdbcSupport {
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
     * 提供一个抽象方法供实现类对单前要执行的sql做一些准备操作
     *
     * @param sql  sql
     * @param args 参数
     * @return 处理后的sql
     */
    protected abstract String getSql(String sql, Map<String, ?> args);

    /**
     * sql翻译帮助
     *
     * @return sql翻译帮助实例
     */
    protected abstract SqlTranslator sqlTranslator();

    /**
     * 是否检查预编译sql对应的参数类型
     *
     * @return 是否检查
     */
    protected abstract boolean checkParameterType();

    /**
     * debug模式输出完整的sql，否则仅输出原始sql
     *
     * @return 是否输出已执行sql
     */
    protected abstract boolean debugFullSql();

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
            throw new UncheckedSqlException("execute sql:\n[" + sql + "]\nerror: ", e);
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
        Pair<String, List<String>> p = compileSql(sql, args);
        final List<String> argNames = p.getItem2();
        final String preparedSql = p.getItem1();
        return execute(preparedSql, sc -> {
            JdbcUtil.setSqlTypedArgs(sc, checkParameterType(), args, argNames);
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
     * 惰性执行一句查询，只有调用终端操作和短路操作才会真正开始执行，
     * 使用完请务必关闭流，否则将一直占用连接对象直到连接池耗尽。<br>
     * 使用{@code try-with-resource}进行包裹：
     * <blockquote>
     * <pre>try ({@link Stream}&lt;{@link DataRow}&gt; stream = queryStream(...)) {
     *       stream.limit(10).forEach(System.out::println);
     *         }</pre>
     * </blockquote>
     * 或者使用完手动关闭流：
     * <blockquote>
     * <pre>
     *     {@link Stream}&lt;{@link DataRow}&gt; stream = queryStream(...);
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
        Pair<String, List<String>> preparedSqlAndArgNames = compileSql(sql, args);
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        final String preparedSql = preparedSqlAndArgNames.getItem1();

        UncheckedCloseable close = null;
        try {
            Connection connection = getConnection();
            // if this query is not in transaction, it's connection managed by Stream
            // if transaction is active connection will not be close when read stream to the end in 'try-with-resource' block
            close = UncheckedCloseable.wrap(() -> releaseConnection(connection, getDataSource()));
            PreparedStatement statement = connection.prepareStatement(preparedSql);
            close = close.nest(statement);
            JdbcUtil.setSqlTypedArgs(statement, checkParameterType(), args, argNames);
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
                        throw new UncheckedSqlException("reading result set of query:[" + preparedSql + "]\nerror: ", ex);
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
            throw new RuntimeException("\nstreaming query error:[" + preparedSql + "]\nargs:" + args, ex);
        }
    }

    /**
     * 批量执行非查询(ddl,dml)语句
     *
     * @param sqls 一组sql
     * @return 每条sql的执行结果
     * @throws UncheckedSqlException         执行批量操作时发生错误
     * @throws UnsupportedOperationException 数据库或驱动版本不支持批量操作
     * @throws IllegalArgumentException      如果执行的sql条数少1条
     */
    public int[] batchExecute(final List<String> sqls) {
        if (sqls.size() > 0) {
            Statement statement = null;
            Connection connection = getConnection();
            if (JdbcUtil.supportsBatchUpdates(connection)) {
                try {
                    statement = connection.createStatement();
                    Map<String, ?> empty = Collections.emptyMap();
                    for (String sql : sqls) {
                        statement.addBatch(getSql(sql, empty));
                    }
                    return statement.executeBatch();
                } catch (SQLException e) {
                    try {
                        JdbcUtil.closeStatement(statement);
                    } catch (SQLException ex) {
                        e.addSuppressed(ex);
                    }
                    statement = null;
                    releaseConnection(connection, getDataSource());
                    throw new UncheckedSqlException("execute batch error: ", e);
                } finally {
                    try {
                        JdbcUtil.closeStatement(statement);
                    } catch (SQLException e) {
                        log.error("close statement error: ", e);
                    }
                    releaseConnection(connection, getDataSource());
                }
            }
            throw new UnsupportedOperationException("your database or jdbc driver not support batch execute currently!");
        }
        throw new IllegalArgumentException("must not be less than one SQL.");
    }

    /**
     * <p>批量执行非查询语句 (insert，update，delete)</p>
     * e.g.
     * <blockquote>
     * <pre>insert into table (a,b,c) values (:v1,:v2,:v3)</pre>
     * <pre>[{v1:'a',v2:'b',v3:'c'},{...},...]</pre>
     * </blockquote>
     *
     * @param sql  sql
     * @param args 一组数据
     * @return 受影响的行数
     */
    public int executeNonQuery(final String sql, Collection<? extends Map<String, ?>> args) {
        if (args.isEmpty()) {
            return 0;
        }
        Map<String, ?> first = args.iterator().next();
        Pair<String, List<String>> preparedSqlAndArgNames = compileSql(sql, first);
        final List<String> argNames = preparedSqlAndArgNames.getItem2();
        final String preparedSql = preparedSqlAndArgNames.getItem1();
        return execute(preparedSql, sc -> {
            Iterator<? extends Map<String, ?>> iterator = args.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                JdbcUtil.setSqlTypedArgs(sc, checkParameterType(), iterator.next(), argNames);
                i += sc.executeUpdate();
            }
            return i;
        });
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
        Pair<String, List<String>> preparedSqlAndArgNames = compileSql(procedure, args);
        final String executeSql = preparedSqlAndArgNames.getItem1();
        final List<String> argNames = preparedSqlAndArgNames.getItem2();

        CallableStatement statement = null;
        Connection connection = getConnection();
        try {
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
                    } else if (CollectionUtil.containsKeyIgnoreCase(args, name)) {
                        @SuppressWarnings("ConstantConditions") ParamMode mode = CollectionUtil.getValueIgnoreCase(args, name).getParamMode();
                        if (mode == ParamMode.OUT || mode == ParamMode.IN_OUT) {
                            outNames.add(name);
                        }
                    }

                }
            }
            statement.execute();
            System.out.println(statement.getObject(1));
            printSqlConsole(statement);
            if (outNames.size() > 0) {
                Object[] values = new Object[outNames.size()];
                int resultIndex = 0;
                for (int i = 0; i < argNames.size(); i++) {
                    String name = argNames.get(i);
                    if (args.containsKey(name)) {
                        if (args.get(name).getParamMode() == ParamMode.OUT || args.get(name).getParamMode() == ParamMode.IN_OUT) {
                            Object result = statement.getObject(i + 1);
                            if (null == result) {
                                values[resultIndex] = null;
                            } else if (result instanceof ResultSet) {
                                List<DataRow> rows = JdbcUtil.createDataRows((ResultSet) result, executeSql, -1);
                                values[resultIndex] = rows;
                                log.debug("boxing a result with type: cursor, convert to ArrayList<DataRow>, get result by name:{}!", outNames.get(resultIndex));
                            } else {
                                values[resultIndex] = result;
                                log.debug("boxing a result, get result by name:{}!", outNames.get(resultIndex));
                            }
                            resultIndex++;
                        }
                    } else if (CollectionUtil.containsKeyIgnoreCase(args, name)) {
                        log.warn("cannot find name: '{}' in args: {}, auto get value by '{}' ignore case, maybe you should check your procedure's named parameter and args.", name, args, name);
                        @SuppressWarnings("ConstantConditions") ParamMode mode = CollectionUtil.getValueIgnoreCase(args, name).getParamMode();
                        if (mode == ParamMode.OUT || mode == ParamMode.IN_OUT) {
                            Object result = statement.getObject(i + 1);
                            if (null == result) {
                                values[resultIndex] = null;
                            } else if (result instanceof ResultSet) {
                                List<DataRow> rows = JdbcUtil.createDataRows((ResultSet) result, executeSql, -1);
                                values[resultIndex] = rows;
                                log.debug("boxing a result with type: cursor, convert to ArrayList<DataRow>, get result by name:{}!", outNames.get(resultIndex));
                            } else {
                                values[resultIndex] = result;
                                log.debug("boxing a result, get result by name:{}!", outNames.get(resultIndex));
                            }
                            resultIndex++;
                        }
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
            throw new UncheckedSqlException("execute procedure [" + procedure + "] error:", e);
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
     * 将自定义的传名参数sql解析为数据库可执行的预编译sql
     *
     * @param sql  sql
     * @param args 参数名
     * @return 预编译sql和参数名
     */
    private Pair<String, List<String>> compileSql(String sql, Map<String, ?> args) {
        if (args == null) {
            args = Collections.emptyMap();
        }
        String sourceSql = getSql(sql, args);
        Pair<String, List<String>> preparedSqlAndArgNames = sqlTranslator().getPreparedSql(sourceSql, args);
        if (log.isDebugEnabled()) {
            log.debug("SQL:{}", SqlUtil.highlightSql(sourceSql));
            log.debug("Args:{}", args);
            if (debugFullSql()) {
                String fullSql = sqlTranslator().generateSql(sourceSql, args, false).getItem1();
                log.debug("Full SQL: {}", SqlUtil.highlightSql(fullSql));
            }
        }
        return preparedSqlAndArgNames;
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
