package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.Jackson;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * JDBC工具类
 */
public class JdbcUtil {

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
        if (Objects.nonNull(obj)) {
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
        if (Objects.nonNull(blob)) {
            try (InputStream ins = blob.getBinaryStream()) {
                bytes = new byte[(int) blob.length()];
                //noinspection ResultOfMethodCallIgnored
                ins.read(bytes);
            } catch (IOException e) {
                throw new UncheckedIOException("read blob catch an error.", e);
            }
        }
        return bytes;
    }

    /**
     * 关闭结果集
     *
     * @param resultSet 结果集
     * @throws SQLException sqlEx
     */
    public static void closeResultSet(ResultSet resultSet) throws SQLException {
        if (Objects.nonNull(resultSet)) {
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
        if (Objects.nonNull(statement)) {
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
        if (Objects.isNull(resultSet)) {
            return Collections.emptyList();
        }
        List<DataRow> list = new ArrayList<>();
        String[] names = null;
        long size = fetchSize;
        while (resultSet.next()) {
            if (size == 0)
                break;
            if (Objects.isNull(names)) {
                names = createNames(resultSet, executedSql);
            }
            list.add(createDataRow(names, resultSet));
            size--;
        }
        return list;
    }

    /**
     * 设置参数占位符的参数值
     *
     * @param ps    预编译对象
     * @param index 序号
     * @param value 值
     * @throws SQLException sqlEx
     */
    public static void setStatementValue(PreparedStatement ps, int index, Object value) throws SQLException {
        if (Objects.isNull(value)) {
            ps.setNull(index, Types.NULL);
        } else if (value instanceof java.util.Date) {
            ps.setObject(index, new Timestamp(((java.util.Date) value).getTime()));
        } else if (value instanceof LocalDateTime) {
            ps.setObject(index, new Timestamp(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalDate) {
            ps.setObject(index, new Date(((LocalDate) value).atStartOfDay(ZoneOffset.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalTime) {
            ps.setObject(index, new Time(((LocalTime) value).atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof OffsetDateTime) {
            ps.setObject(index, new Timestamp(((OffsetDateTime) value).toInstant().toEpochMilli()));
        } else if (value instanceof OffsetTime) {
            ps.setObject(index, new Time(((OffsetTime) value).atDate(LocalDate.now()).toInstant().toEpochMilli()));
        } else if (value instanceof ZonedDateTime) {
            ps.setObject(index, new Timestamp(((ZonedDateTime) value).toInstant().toEpochMilli()));
        } else if (value instanceof Instant) {
            ps.setObject(index, new Timestamp(((Instant) value).toEpochMilli()));
        } else if (value instanceof UUID) {
            ps.setObject(index, value.toString().replace("-", ""));
        } else if (value instanceof Map || value instanceof Collection) {
            ps.setObject(index, Jackson.toJson(value));
        } else if (!value.getClass().getTypeName().startsWith("java.")) {
            ps.setObject(index, Jackson.toJson(value));
        } else if (value instanceof InputStream) {
            ps.setBinaryStream(index, (InputStream) value);
        } else if (value instanceof Path) {
            try {
                ps.setBinaryStream(index, Files.newInputStream((Path) value));
            } catch (IOException e) {
                throw new SQLException("set binary value failed.", e);
            }
        } else if (value instanceof File) {
            try {
                ps.setBinaryStream(index, new FileInputStream((File) value));
            } catch (FileNotFoundException e) {
                throw new SQLException("set binary value failed.", e);
            }
        } else {
            ps.setObject(index, value);
        }
    }
}
