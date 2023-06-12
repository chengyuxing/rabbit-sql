package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.common.utils.CollectionUtil;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ParamMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;

import static com.github.chengyuxing.common.utils.CollectionUtil.containsKeyIgnoreCase;
import static com.github.chengyuxing.common.utils.CollectionUtil.getValueIgnoreCase;
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
     * @throws SQLException     ex
     * @throws RuntimeException 如果通过反射获取PgArray对象出现错误
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
            obj = clob.getSubString(1, (int) clob.length());
        } else if ("org.postgresql.jdbc.PgArray".equals(className)) {
            obj = resultSet.getArray(index).getArray();
        } else if ("org.postgresql.util.PGobject".equals(className)) {
            obj = resultSet.getString(index);
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
                throw new UncheckedIOException("read blob catch an error:", e);
            }
        }
        return bytes;
    }

    /**
     * 判断是否支持批量执行修改操作
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
            log.error("JDBC driver 'supportsBatchUpdates' method threw exception", ex);
        }
        return false;
    }

    /**
     * 关闭结果集
     *
     * @param resultSet 结果集
     * @throws SQLException sqlEx
     */
    public static void closeResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet != null) {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }
        }
    }

    /**
     * 关闭connection声明对象
     *
     * @param statement 声明对象
     * @throws SQLException sqlEx
     */
    public static void closeStatement(Statement statement) throws SQLException {
        if (statement != null) {
            if (!statement.isClosed()) {
                statement.close();
            }
        }
    }

    /**
     * 创建数据行表头
     *
     * @param resultSet   结果集
     * @param executedSql 将要执行的原生sql，用于识别双引号包含的字段不进行转小写操作
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
        DataRow row = new DataRow(names.length);
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            if (name.equals("?column?")) {
                name = "column" + i;
            }
            row.put(name, JdbcUtil.getResultValue(resultSet, i + 1));
        }
        return row;
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
        } catch (IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException |
                 InstantiationException e) {
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
        ParameterMetaData pmd;
        String pClass;
        String pTypeName;
        int pType;
        try {
            pmd = statement.getParameterMetaData();
            pClass = pmd.getParameterClassName(index);
            pTypeName = pmd.getParameterTypeName(index);
            pType = pmd.getParameterType(index);
        } catch (SQLException e) {
            // 如果数据库不支持此特性，记录错误，给出提示，并继续完成操作，而不是直接打断
            log.warn("maybe jdbc driver not support the check parameter type, set 'checkParameterType' false to disabled the check: ", e);
            log.warn("auto switch normal mode to set value...");
            setStatementValue(statement, index, value);
            return;
        }
        if (null == value) {
            statement.setNull(index, pType);
        } else {
            // if postgresql, insert as json(b) type
            // if column is json type
            if (pTypeName.equals("json") || pTypeName.equals("jsonb")) {
                log.warn("you try to set a value into json(b) type field, auto wrap for json(b) type!");
                if (value instanceof String) {
                    statement.setObject(index, createPgObject(pTypeName, value.toString()));
                } else {
                    statement.setObject(index, createPgObject(pTypeName, obj2Json(value)));
                }
            } else if (pClass.equals("java.lang.String") && !(value instanceof String)) {
                if (value instanceof Map || value instanceof Collection) {
                    log.warn("you try to set a Map or Collection data into database string type field, auto convert to json string!");
                    statement.setObject(index, obj2Json(value));
                    // maybe Date, LocalDateTime, UUID, BigDecimal, Integer...
                } else if (value instanceof UUID) {
                    statement.setObject(index, value.toString().replace("-", ""));
                } else if (value.getClass().getTypeName().startsWith("java.")) {
                    statement.setObject(index, value.toString());
                } else {
                    // maybe is java bean
                    log.warn("you try to set an unknown class instance(maybe your java bean) data into string type field, auto convert to json string!");
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
            statement.setObject(index, new Timestamp(((java.util.Date) value).getTime()));
        } else if (value instanceof LocalDateTime) {
            statement.setObject(index, new Timestamp(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalDate) {
            statement.setObject(index, new Date(((LocalDate) value).atStartOfDay(ZoneOffset.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalTime) {
            statement.setObject(index, new Time(((LocalTime) value).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof Instant) {
            statement.setObject(index, new Timestamp(((Instant) value).toEpochMilli()));
        } else if (value instanceof UUID) {
            statement.setObject(index, value.toString().replace("-", ""));
        } else if (value instanceof Map || value instanceof Collection) {
            log.warn("you try to set a Map or Collection data, auto convert to json string!");
            statement.setObject(index, obj2Json(value));
        } else if (!value.getClass().getTypeName().startsWith("java.")) {
            log.warn("you try to set an unknown class instance(maybe your java bean) data, auto convert to json string!");
            statement.setObject(index, obj2Json(value));
        } else if (value instanceof InputStream) {
            statement.setBinaryStream(index, (InputStream) value);
        } else if (value instanceof Path) {
            try {
                statement.setBinaryStream(index, Files.newInputStream((Path) value));
            } catch (IOException e) {
                throw new SQLException("set binary value failed: ", e);
            }
        } else if (value instanceof File) {
            try {
                statement.setBinaryStream(index, new FileInputStream((File) value));
            } catch (FileNotFoundException e) {
                throw new SQLException("set binary value failed: ", e);
            }
        } else {
            statement.setObject(index, value);
        }
    }

    /**
     * 判断数据库表字段类型并对应注册预编译sql参数
     *
     * @param statement          sql声明
     * @param checkParameterType 是否检查数据库字段参数对应类型
     * @param args               参数
     * @param names              占位符参数名
     * @throws SQLException ex
     */
    public static void setSqlTypedArgs(PreparedStatement statement, boolean checkParameterType, Map<String, ?> args, List<String> names) throws SQLException {
        if (args != null && !args.isEmpty()) {
            if (checkParameterType) {
                for (int i = 0; i < names.size(); i++) {
                    int index = i + 1;
                    String name = names.get(i);
                    if (args.containsKey(name)) {
                        setSpecialStatementValue(statement, index, args.get(name));
                    } else if (containsKeyIgnoreCase(args, name)) {
                        log.warn("cannot find name: '{}' in args: {}, auto get value by '{}' ignore case, maybe you should check your sql's named parameter and args.", name, args, name);
                        setSpecialStatementValue(statement, index, getValueIgnoreCase(args, name));
                    }
                }
            } else {
                setSqlPoolArgs(statement, args, names);
            }
        }
    }

    /**
     * 不判断数据库表字段类型注册预编译sql参数
     *
     * @param statement sql声明
     * @param args      参数
     * @param names     占位符参数名
     * @throws SQLException ex
     */
    public static void setSqlPoolArgs(PreparedStatement statement, Map<String, ?> args, List<String> names) throws SQLException {
        for (int i = 0; i < names.size(); i++) {
            int index = i + 1;
            String name = names.get(i);
            if (args.containsKey(name)) {
                setStatementValue(statement, index, args.get(name));
            } else if (containsKeyIgnoreCase(args, name)) {
                log.warn("cannot find name: '{}' in args: {}, auto get value by '{}' ignore case, maybe you should check your sql's named parameter and args.", name, args, name);
                setStatementValue(statement, index, getValueIgnoreCase(args, name));
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
            // adapt postgresql
            // out and inout param first
            for (int i = 0; i < names.size(); i++) {
                int index = i + 1;
                String name = names.get(i);
                if (args.containsKey(name) || containsKeyIgnoreCase(args, name)) {
                    Param param = args.containsKey(name) ? args.get(name) : CollectionUtil.getValueIgnoreCase(args, name);
                    if (param != null) {
                        if (param.getParamMode() == ParamMode.OUT || param.getParamMode() == ParamMode.IN_OUT) {
                            statement.registerOutParameter(index, param.getType().getTypeNumber());
                        }
                    }
                }
            }
            // in param next
            for (int i = 0; i < names.size(); i++) {
                int index = i + 1;
                String name = names.get(i);
                if (args.containsKey(name) || containsKeyIgnoreCase(args, name)) {
                    Param param = args.containsKey(name) ? args.get(name) : CollectionUtil.getValueIgnoreCase(args, name);
                    if (param != null) {
                        if (param.getParamMode() == ParamMode.IN || param.getParamMode() == ParamMode.IN_OUT) {
                            setStatementValue(statement, index, param.getValue());
                        }
                    }
                }
            }
        }
    }
}
