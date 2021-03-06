package com.github.chengyuxing.sql.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.sql.exceptions.SqlRuntimeException;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;

import static com.github.chengyuxing.common.utils.ReflectUtil.obj2Json;

/**
 * JDBC工具类
 */
public class JdbcUtil {
    private final static Logger log = LoggerFactory.getLogger(JdbcUtil.class);
    private static Class<?> pgObjClass;
    private static Method pgObjSetValue;
    private static Method pgObjSetType;

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
            throw new SqlRuntimeException("close result error:", e);
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
            throw new SqlRuntimeException("close statement error:", e);
        }
    }

    /**
     * 创建数据行表头
     *
     * @param resultSet   结果集
     * @param executedSql 将要执行的原生sql
     * @return 一组表头
     * @throws SQLException 数据库异常
     */
    public static String[] createNames(ResultSet resultSet, final String executedSql) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        String[] names = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (executedSql.contains("\"" + columnName + "\"")) {
                names[i] = columnName;
            } else {
                names[i] = columnName.toLowerCase();
            }
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
        Object[] values = new Object[names.length];
        for (int i = 0; i < names.length; i++) {
            values[i] = JdbcUtil.getResultValue(resultSet, i + 1);
        }
        return DataRow.of(names, values);
    }

    /**
     * 解析ResultSet
     *
     * @param resultSet   结果集
     * @param executedSql 将要执行的原生sql
     * @param fetchSize   请求数据大小
     * @return 以流包装的结果集
     * @throws SQLException ex
     */
    public static List<DataRow> createDataRows(final ResultSet resultSet, final String executedSql, final long fetchSize) throws SQLException {
        if (resultSet == null) {
            return Collections.emptyList();
        }
        List<DataRow> list = new ArrayList<>();
        String[] names = null;
        long size = fetchSize;
        while (resultSet.next()) {
            if (size == 0)
                break;
            if (names == null) {
                names = createNames(resultSet, executedSql);
            }
            list.add(createDataRow(names, resultSet));
            size--;
        }
        closeResultSet(resultSet);
        return list;
    }

    /**
     * 创建PostgreSQL的对象类型数据
     *
     * @param type  字段类型
     * @param value 值
     * @return PostgreSQL对象
     */
    public static Object createPgObject(String type, String value) {
        try {
            if (pgObjClass == null) {
                pgObjClass = Class.forName("org.postgresql.util.PGobject");
            }
            if (pgObjSetType == null) {
                pgObjSetType = pgObjClass.getDeclaredMethod("setType", String.class);
            }
            if (pgObjSetValue == null) {
                pgObjSetValue = pgObjClass.getDeclaredMethod("setValue", String.class);
            }
            Object pgObj = pgObjClass.newInstance();
            pgObjSetType.invoke(pgObj, type);
            pgObjSetValue.invoke(pgObj, value);
            return pgObj;
        } catch (IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException("create postgresql object type error:", e);
        }
    }

    /**
     * 设置特殊的预编译sql的参数值，可以自动转一些换合适的数据类型
     *
     * @param statement statement
     * @param index     序号
     * @param value     值
     * @throws SQLException sqlExp
     */
    public static void setSpecialStatementValue(PreparedStatement statement, int index, Object value) throws SQLException {
        ParameterMetaData pmd = statement.getParameterMetaData();
        String pClass = pmd.getParameterClassName(index);
        String pType = pmd.getParameterTypeName(index);
        if (null == value) {
            statement.setNull(index, pmd.getParameterType(index));
        } else {
            // if postgresql, insert as json(b) type
            // if column is json type
            if (pType.equals("json") || pType.equals("jsonb")) {
                if (value instanceof String) {
                    statement.setObject(index, createPgObject(pType, value.toString()));
                } else {
                    statement.setObject(index, createPgObject(pType, obj2Json(value)));
                }
            } else if (pClass.equals("java.lang.String") && !(value instanceof String)) {
                if (value instanceof Map || value instanceof Collection) {
                    statement.setObject(index, obj2Json(value));
                    // maybe Date, LocalDateTime, UUID, BigDecimal, Integer...
                } else if (value.getClass().getTypeName().startsWith("java.")) {
                    statement.setObject(index, value.toString());
                } else {
                    // maybe is java bean
                    statement.setObject(index, obj2Json(value));
                }
                // if is postgresql array
            } else if (pClass.equals("java.sql.Array") && value instanceof Collection) {
                statement.setObject(index, ((Collection<?>) value).toArray());
            } else if (pClass.equals("java.sql.Date") && value instanceof String) {
                statement.setObject(index, new Date(DateTimes.toEpochMilli((String) value)));
            } else if (pClass.equals("java.sql.Time") && value instanceof String) {
                statement.setObject(index, new Time(DateTimes.toEpochMilli((String) value)));
            } else if (pClass.equals("java.sql.Timestamp") && value instanceof String) {
                statement.setObject(index, new Timestamp(DateTimes.toEpochMilli((String) value)));
            } else {
                setStatementValue(statement, index, value);
            }
        }
    }

    /**
     * 设置常规的参数占位符的参数值
     *
     * @param statement statement
     * @param index     序号
     * @param value     值
     * @throws SQLException sqlExp
     */
    public static void setStatementValue(PreparedStatement statement, int index, Object value) throws SQLException {
        if (null == value) {
            statement.setNull(index, Types.NULL);
        } else if (value instanceof java.util.Date) {
            statement.setObject(index, new Date(((java.util.Date) value).getTime()));
        } else if (value instanceof LocalDateTime) {
            statement.setObject(index, new Timestamp(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalDate) {
            statement.setObject(index, new Date(((LocalDate) value).atStartOfDay(ZoneOffset.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalTime) {
            statement.setObject(index, new Time(((LocalTime) value).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof Instant) {
            statement.setObject(index, new Timestamp(((Instant) value).toEpochMilli()));
        } else if (value instanceof InputStream) {
            statement.setBinaryStream(index, (InputStream) value);
        } else if (value instanceof File) {
            try {
                statement.setBinaryStream(index, new FileInputStream((File) value));
            } catch (FileNotFoundException e) {
                throw new SqlRuntimeException("set value failed:", e);
            }
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
                int index = i + 1;
                String name = names.get(i);
                if (args.containsKey(name)) {
                    setSpecialStatementValue(statement, index, args.get(name));
                } else if (args.containsKey(":" + name)) {
                    setSpecialStatementValue(statement, index, args.get(":" + name));
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
            // out and inout param first
            for (int i = 0; i < names.size(); i++) {
                int index = i + 1;
                String name = names.get(i);
                Param param = null;
                if (args.containsKey(name)) {
                    param = args.get(name);
                } else if (args.containsKey(":" + name)) {
                    param = args.get(":" + name);
                }
                if (param != null) {
                    if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                        statement.registerOutParameter(index, param.getType().getTypeNumber());
                    }
                }
            }
            // in param first
            for (int i = 0; i < names.size(); i++) {
                int index = i + 1;
                String name = names.get(i);
                Param param = null;
                if (args.containsKey(name)) {
                    param = args.get(name);
                } else if (args.containsKey(":" + name)) {
                    param = args.get(":" + name);
                }
                if (param != null) {
                    if (param.getParamMode() == ParamMode.IN || param.getParamMode() == ParamMode.IN_OUT) {
                        setStatementValue(statement, index, param.getValue());
                    }
                }
            }
        }
    }
}
