package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
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
public class JdbcUtils {
    private static final Logger log = LoggerFactory.getLogger(JdbcUtils.class);

    public static Object getResultValue(@Nullable ResultSet resultSet, @Range(from = 1, to = Integer.MAX_VALUE) int index) throws SQLException {
        if (resultSet == null) {
            return null;
        }
        Object obj = resultSet.getObject(index);
        String className = null;
        if (obj != null) {
            className = obj.getClass().getName();
        }
        if (obj instanceof Blob) {
            try {
                obj = FileResource.readBytes(((Blob) obj).getBinaryStream());
            } catch (IOException e) {
                throw new UncheckedIOException("Read blob error.", e);
            }
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

    public static void setStatementValue(@NotNull PreparedStatement ps, @Range(from = 1, to = Integer.MAX_VALUE) int index, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
        } else if (value instanceof java.util.Date) {
            ps.setTimestamp(index, Timestamp.from(((java.util.Date) value).toInstant()));
        } else if (value instanceof LocalDateTime) {
            ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
        } else if (value instanceof LocalDate) {
            ps.setDate(index, Date.valueOf((LocalDate) value));
        } else if (value instanceof LocalTime) {
            ps.setTime(index, Time.valueOf((LocalTime) value));
        } else if (value instanceof OffsetDateTime) {
            ps.setTimestamp(index, Timestamp.from(((OffsetDateTime) value).toInstant()));
        } else if (value instanceof OffsetTime) {
            ps.setTime(index, Time.valueOf(((OffsetTime) value).toLocalTime()));
        } else if (value instanceof ZonedDateTime) {
            ps.setTimestamp(index, Timestamp.from(((ZonedDateTime) value).toInstant()));
        } else if (value instanceof Instant) {
            ps.setTimestamp(index, Timestamp.from((Instant) value));
        } else if (value instanceof UUID) {
            ps.setString(index, value.toString().replace("-", ""));
        } else if (value instanceof InputStream) {
            ps.setBinaryStream(index, (InputStream) value);
        } else if (value instanceof Path) {
            try {
                ps.setBinaryStream(index, Files.newInputStream((Path) value));
            } catch (IOException e) {
                throw new IllegalArgumentException("Set binary value failed.", e);
            }
        } else if (value instanceof File) {
            try {
                ps.setBinaryStream(index, new FileInputStream((File) value));
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException("Set binary value failed.", e);
            }
        } else {
            ps.setObject(index, value);
        }
    }

    /**
     * Do execute query, ddl, dml or plsql statement to get result.<br>
     * Get a result by index {@code 0} or by name: {@code result} .
     *
     * @param statement preparedStatement
     * @param sql       executed sql
     * @return DataRow
     * @throws SQLException sql exp
     */
    public static DataRow getResult(@NotNull PreparedStatement statement, @NotNull final String sql) throws SQLException {
        ResultSet resultSet = statement.getResultSet();
        if (resultSet != null) {
            List<DataRow> result = createDataRows(resultSet, sql, -1);
            closeResultSet(resultSet);
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
    public static void printSqlConsole(@NotNull Statement sc) {
        if (log.isWarnEnabled()) {
            try {
                SQLWarning warning = sc.getWarnings();
                if (warning != null) {
                    String state = warning.getSQLState();
                    warning.forEach(r -> log.warn("[{}] [{}] {}", LocalDateTime.now(), state, r.getMessage()));
                }
            } catch (SQLException e) {
                log.debug("get sql warning error.", e);
            }
        }
    }

    public static void closeResultSet(@Nullable ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.debug("Close result error.", e);
            }
        }
    }

    public static void closeStatement(@Nullable Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.debug("Close statement error.", e);
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
    public static String[] createNames(@NotNull ResultSet resultSet, @NotNull final String executedSql) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        String[] names = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String columnName = metaData.getColumnLabel(i + 1);
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
    public static DataRow createDataRow(String[] names, @Nullable ResultSet resultSet) throws SQLException {
        DataRow row = new DataRow(names.length);
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
    public static List<DataRow> createDataRows(@Nullable final ResultSet resultSet, @NotNull final String executedSql, @Range(from = -1, to = Long.MAX_VALUE) final long fetchSize) throws SQLException {
        if (resultSet == null) {
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
