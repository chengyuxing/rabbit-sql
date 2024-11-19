package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.MostDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * JDBC util.
 */
public class JdbcUtil {
    private static final Logger log = LoggerFactory.getLogger(JdbcUtil.class);

    public static Object getResultValue(ResultSet resultSet, int index) throws SQLException {
        Object obj = resultSet.getObject(index);
        String className = null;
        if (Objects.nonNull(obj)) {
            className = obj.getClass().getName();
        }
        if (obj instanceof Blob) {
            obj = getBytes((Blob) obj);
        } else if (obj instanceof Clob clob) {
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

    public static void setStatementValue(PreparedStatement ps, int index, Object value) throws SQLException {
        if (Objects.isNull(value)) {
            ps.setNull(index, Types.NULL);
        } else if (value instanceof java.util.Date date) {
            ps.setObject(index, new Timestamp(date.getTime()));
        } else if (value instanceof LocalDateTime localDateTime) {
            ps.setObject(index, new Timestamp(localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalDate localDate) {
            ps.setObject(index, new Date(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof LocalTime localTime) {
            ps.setObject(index, new Time(localTime.atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        } else if (value instanceof OffsetDateTime offsetDateTime) {
            ps.setObject(index, new Timestamp(offsetDateTime.toInstant().toEpochMilli()));
        } else if (value instanceof OffsetTime offsetTime) {
            ps.setObject(index, new Time(offsetTime.atDate(LocalDate.now()).toInstant().toEpochMilli()));
        } else if (value instanceof ZonedDateTime zonedDateTime) {
            ps.setObject(index, new Timestamp(zonedDateTime.toInstant().toEpochMilli()));
        } else if (value instanceof Instant instant) {
            ps.setObject(index, new Timestamp(instant.toEpochMilli()));
        } else if (value instanceof MostDateTime mostDateTime) {
            ps.setObject(index, new Timestamp(mostDateTime.toInstant().toEpochMilli()));
        } else if (value instanceof UUID) {
            ps.setObject(index, value.toString().replace("-", ""));
        } else if (value instanceof InputStream inputStream) {
            ps.setBinaryStream(index, inputStream);
        } else if (value instanceof Path path) {
            try {
                ps.setBinaryStream(index, Files.newInputStream(path));
            } catch (IOException e) {
                throw new SQLException("set binary value failed.", e);
            }
        } else if (value instanceof File file) {
            try {
                ps.setBinaryStream(index, new FileInputStream(file));
            } catch (FileNotFoundException e) {
                throw new SQLException("set binary value failed.", e);
            }
        } else {
            ps.setObject(index, value);
        }
    }

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
     * Do execute query, ddl, dml or plsql statement to get result.<br>
     * Get result by index {@code 0} or by name: {@code result} .
     *
     * @param statement preparedStatement
     * @param sql       executed sql
     * @return DataRow
     * @throws SQLException sql exp
     */
    public static DataRow getResult(PreparedStatement statement, final String sql) throws SQLException {
        var resultSet = statement.getResultSet();
        if (Objects.nonNull(resultSet)) {
            var result = JdbcUtil.createDataRows(resultSet, sql, -1);
            JdbcUtil.closeResultSet(resultSet);
            return DataRow.of("result", result, "type", "QUERY");
        }
        int i = statement.getUpdateCount();
        if (i != -1) {
            return DataRow.of("result", i, "type", "DD(M)L");
        }
        return new DataRow(0);
    }

    /**
     * Print sql log, e.g. postgresql:
     * <blockquote><pre>
     * raise notice 'my console.';</pre>
     * </blockquote>
     *
     * @param sc sql statement object
     */
    public static void printSqlConsole(Statement sc) {
        if (log.isWarnEnabled()) {
            try {
                var warning = sc.getWarnings();
                if (warning != null) {
                    var state = warning.getSQLState();
                    warning.forEach(r -> log.warn("[{}] [{}] {}", LocalDateTime.now(), state, r.getMessage()));
                }
            } catch (SQLException e) {
                log.error("get sql warning error.", e);
            }
        }
    }

    public static void closeResultSet(ResultSet resultSet) throws SQLException {
        if (Objects.nonNull(resultSet)) {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }
        }
    }

    public static void closeStatement(Statement statement) throws SQLException {
        if (Objects.nonNull(statement)) {
            if (!statement.isClosed()) {
                statement.close();
            }
        }
    }

    /**
     * Create fields array by query resulSet.
     *
     * @param resultSet   query resulSet
     * @param executedSql executed query sql, check column which be double-quoted for exclude case-sensitive column
     * @return fields array
     * @throws SQLException ex
     */
    public static String[] createNames(ResultSet resultSet, final String executedSql) throws SQLException {
        var metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        String[] names = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (!executedSql.contains("\"" + columnName + "\"")) {
                columnName = columnName.toLowerCase();
            }
            if (columnName.equals("?column?")) {
                columnName = "column" + i;
            }
            names[i] = columnName;
        }
        return names;
    }

    /**
     * Create DataRow from resultSet.
     *
     * @param names     fields array
     * @param resultSet resultSet
     * @return DataRow
     * @throws SQLException ex
     */
    public static DataRow createDataRow(String[] names, ResultSet resultSet) throws SQLException {
        var row = new DataRow(names.length);
        for (int i = 0; i < names.length; i++) {
            row.put(names[i], getResultValue(resultSet, i + 1));
        }
        return row;
    }

    /**
     * Create DataRows from resultSet.
     *
     * @param resultSet   resultSet
     * @param executedSql executed query sql, check column which be double-quoted for exclude case-sensitive column
     * @param fetchSize   request result set size
     * @return DataRows
     * @throws SQLException ex
     */
    public static List<DataRow> createDataRows(final ResultSet resultSet, final String executedSql, final long fetchSize) throws SQLException {
        if (Objects.isNull(resultSet)) {
            return Collections.emptyList();
        }
        List<DataRow> list = new ArrayList<>();
        String[] names = createNames(resultSet, executedSql);
        long size = fetchSize;
        while (resultSet.next()) {
            if (size == 0)
                break;
            list.add(createDataRow(names, resultSet));
            size--;
        }
        return list;
    }
}
