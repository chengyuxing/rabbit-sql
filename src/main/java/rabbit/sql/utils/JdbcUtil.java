package rabbit.sql.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.common.types.DataRow;
import rabbit.sql.dao.Wrap;
import rabbit.sql.types.Param;
import rabbit.sql.types.ParamMode;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static rabbit.sql.utils.SqlUtil.unwrapValue;

/**
 * JDBC工具类
 */
public class JdbcUtil {
    private final static Logger log = LoggerFactory.getLogger(JdbcUtil.class);

    /**
     * 获取result结果
     *
     * @param resultSet resultSet
     * @param index     序号
     * @return java类型值
     * @throws SQLException ex
     */
    public static Object getResultValue(ResultSet resultSet, int index) throws SQLException {
        Object obj = resultSet.getObject(index);
        String className = null;
        if (obj != null) {
            className = obj.getClass().getName();
        }
        if (obj instanceof Blob) {
            obj = getBytes((Blob) obj);
        } else if (obj instanceof Clob) {
            Clob clob = (Clob) obj;
            obj = clob.getSubString(0, (int) clob.length());
        } else if ("org.postgresql.util.PGobject".equals(className)) {
            obj = resultSet.getString(index);
        } else if ("org.postgresql.jdbc.PgArray".equals(className)) {
            try {
                Method method = obj.getClass().getDeclaredMethod("getArray");
                obj = method.invoke(obj);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                log.error("invoke PgArray.getArray() with wrong:{}", e.getMessage());
            }
        } else if ("oracle.sql.TIMESTAMP".equals(className) || "oracle.sql.TIMESTAMPTZ".equals(className)) {
            obj = resultSet.getTimestamp(index);
        } else if (className != null && className.startsWith("oracle.sql.DATE")) {
            String metaDataClassName = resultSet.getMetaData().getColumnClassName(index);
            if ("java.sql.Timestamp".equals(metaDataClassName) || "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
                obj = resultSet.getTimestamp(index);
            } else {
                obj = resultSet.getDate(index);
            }
        } else if (obj instanceof java.sql.Date) {
            if ("java.sql.Timestamp".equals(resultSet.getMetaData().getColumnClassName(index))) {
                obj = resultSet.getTimestamp(index);
            }
        }
        return obj;
    }

    /**
     * Blob对象转换为字节数组
     *
     * @param blob 二进制对象
     * @return 字节数组
     * @throws SQLException SqlExp
     */
    public static byte[] getBytes(Blob blob) throws SQLException {
        byte[] bytes = new byte[0];
        if (blob != null) {
            try (InputStream ins = blob.getBinaryStream()) {
                bytes = new byte[(int) blob.length()];
                //noinspection ResultOfMethodCallIgnored
                ins.read(bytes);
            } catch (IOException e) {
                log.error("read blob catch an error:" + e.getMessage());
            }
        }
        return bytes;
    }

    /**
     * 判断是否支持存储过程和函数
     *
     * @param connection 连接对象
     * @return 是否支持
     */
    public static boolean supportStoredProcedure(Connection connection) {
        try {
            DatabaseMetaData dbmd = connection.getMetaData();
            if (dbmd != null) {
                if (dbmd.supportsStoredProcedures()) {
                    log.debug("JDBC driver supports stored procedure");
                    return true;
                } else {
                    log.debug("JDBC driver does not support stored procedure");
                }
            }
        } catch (SQLException throwables) {
            log.debug("JDBC driver 'supportsStoredProcedures' method threw exception", throwables);
        }
        return false;
    }

    /**
     * 判断是否支持命名参数
     *
     * @param connection 连接对象
     * @return 是否支持
     */
    public static boolean supportsNamedParameters(Connection connection) {
        try {
            DatabaseMetaData dbmd = connection.getMetaData();
            if (dbmd != null) {
                if (dbmd.supportsNamedParameters()) {
                    log.debug("JDBC driver supports stored procedure");
                    return true;
                } else {
                    log.debug("JDBC driver does not support stored procedure");
                }
            }
        } catch (SQLException throwables) {
            log.debug("JDBC driver 'supportsStoredProcedures' method threw exception", throwables);
        }
        return false;
    }

    /**
     * 判断是非支持批量执行修改操作
     *
     * @param con 连接对象
     * @return 是否支持
     */
    public static boolean supportsBatchUpdates(Connection con) {
        try {
            DatabaseMetaData dbmd = con.getMetaData();
            if (dbmd != null) {
                if (dbmd.supportsBatchUpdates()) {
                    log.debug("JDBC driver supports batch updates");
                    return true;
                } else {
                    log.debug("JDBC driver does not dao batch updates");
                }
            }
        } catch (SQLException ex) {
            log.debug("JDBC driver 'supportsBatchUpdates' method threw exception", ex);
        }
        return false;
    }

    /**
     * 关闭结果集
     *
     * @param resultSet 结果集
     */
    public static void closeResultSet(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                if (!resultSet.isClosed()) {
                    resultSet.close();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭connection声明对象
     *
     * @param statement 声明对象
     */
    public static void closeStatement(Statement statement) {
        try {
            if (statement != null) {
                if (!statement.isClosed()) {
                    statement.close();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建数据行表头
     *
     * @param resultSet 结果集
     * @return 一组表头
     * @throws SQLException sqlEx
     */
    public static String[] createNames(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        String[] names = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            names[i] = metaData.getColumnName(i + 1).toLowerCase();
        }
        return names;
    }

    /**
     * 创建数据行
     *
     * @param names     表头名
     * @param resultSet 结果集
     * @return 数据化载体
     * @throws SQLException sqlEx
     */
    public static DataRow createDataRow(String[] names, ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String[] types = new String[names.length];
        Object[] values = new Object[names.length];
        for (int i = 0; i < names.length; i++) {
            values[i] = JdbcUtil.getResultValue(resultSet, i + 1);
            if (values[i] == null) {
                types[i] = metaData.getColumnClassName(i + 1);
            } else {
                types[i] = values[i].getClass().getName();
            }
        }
        return DataRow.of(names, types, values);
    }

    /**
     * 解析ResultSet
     *
     * @param resultSet 结果集
     * @param fetchSize 请求数据大小
     * @return 以流包装的结果集
     * @throws SQLException ex
     */
    public static List<DataRow> resolveResultSet(final ResultSet resultSet, final long fetchSize) throws SQLException {
        List<DataRow> list = new ArrayList<>();
        String[] names = null;
        long size = fetchSize;
        while (resultSet.next()) {
            if (size == 0)
                break;
            if (names == null) {
                names = createNames(resultSet);
            }
            list.add(createDataRow(names, resultSet));
            size--;
        }
        closeResultSet(resultSet);
        return list;
    }

    /**
     * 设置预编译sql的参数值
     *
     * @param statement statement
     * @param index     序号
     * @param value     值
     * @throws SQLException sqlExp
     */
    public static void setStatementValue(PreparedStatement statement, int index, Object value) throws SQLException {
        if (value instanceof java.util.Date) {
            statement.setObject(index, new Date(((java.util.Date) value).getTime()));
        } else if (value instanceof LocalDateTime) {
            statement.setObject(index, new Timestamp(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalDate) {
            statement.setObject(index, new Date(((LocalDate) value).atStartOfDay(ZoneOffset.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalTime) {
            statement.setObject(index, new Time(((LocalTime) value).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof Instant) {
            statement.setObject(index, new Timestamp(((Instant) value).toEpochMilli()));
        } else if (value instanceof Wrap) {
            statement.setObject(index, unwrapValue(value));
        } else {
            statement.setObject(index, value);
        }
    }

    /**
     * 注册预编译sql参数
     *
     * @param statement sql声明
     * @param args      参数
     * @param names     占位符参数名
     * @throws SQLException ex
     */
    public static void setSqlArgs(PreparedStatement statement, Map<String, Object> args, List<String> names) throws SQLException {
        if (args != null && !args.isEmpty()) {
            for (int i = 0; i < names.size(); i++) {
                if (args.containsKey(names.get(i))) {
                    int index = i + 1;
                    Object param = args.get(names.get(i));
                    setStatementValue(statement, index, param);
                }
            }
        }
    }

    /**
     * 注册预编译存储过程参数
     *
     * @param statement 声明
     * @param args      参数
     * @param names     占位符参数名
     * @throws SQLException ex
     */
    public static void setStoreArgs(CallableStatement statement, Map<String, Param> args, List<String> names) throws SQLException {
        if (args != null && !args.isEmpty()) {
            for (int i = 0; i < names.size(); i++) {
                if (args.containsKey(names.get(i))) {
                    int index = i + 1;
                    Param param = args.get(names.get(i));
                    if (param.getParamMode() == ParamMode.IN) {
                        setStatementValue(statement, index, param.getValue());
                    } else if (param.getParamMode() == ParamMode.OUT) {
                        statement.registerOutParameter(index, param.getType().getTypeNumber());
                    } else if (param.getParamMode() == ParamMode.IN_OUT) {
                        setStatementValue(statement, index, param.getValue());
                        statement.registerOutParameter(index, param.getType().getTypeNumber());
                    }
                }
            }
        }
    }
}
