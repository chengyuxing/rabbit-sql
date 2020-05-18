package rabbit.sql.utils;

import rabbit.common.types.DataRow;
import rabbit.sql.support.JdbcSupport;
import rabbit.sql.types.ParamMode;
import rabbit.sql.types.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class JdbcUtil {
    private final static Logger log = LoggerFactory.getLogger(JdbcSupport.class);

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

    public static byte[] getBytes(Blob blob) throws SQLException {
        byte[] bytes = new byte[0];
        if (blob != null) {
            try (InputStream ins = blob.getBinaryStream()) {
                bytes = new byte[(int) blob.length()];
                ins.read(bytes);
            } catch (IOException e) {
                log.error("read blob catch an error:" + e.getMessage());
            }
        }
        return bytes;
    }

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
     * 解析ResultSet
     *
     * @param resultSet 结果集
     * @param fetchSize 请求数据大小
     * @param convert   转换
     * @param <T>       目标类型
     * @return 以流包装的结果集
     * @throws SQLException ex
     */
    public static <T> Stream<T> resolveResultSet(final ResultSet resultSet, final long fetchSize, final Function<DataRow, T> convert) throws SQLException {
        Stream.Builder<T> sb = Stream.builder();
        long size = fetchSize;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        String[] types = new String[columnCount];
        String[] fields = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            fields[i] = metaData.getColumnName(i + 1).toLowerCase();
        }
        boolean first = true;
        while (resultSet.next()) {
            if (size == 0)
                break;
            Object[] values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = JdbcUtil.getResultValue(resultSet, i + 1);
            }
            if (first) {
                for (int i = 0; i < columnCount; i++) {
                    if (values[i] == null) {
                        types[i] = metaData.getColumnClassName(i + 1);
                    } else {
                        types[i] = values[i].getClass().getName();
                    }
                }
                first = false;
            }
            sb.accept(convert.apply(DataRow.of(fields, types, values)));
            size--;
        }
        closeResultSet(resultSet);
        return sb.build();
    }

    /**
     * 注册预编译SQL参数
     *
     * @param statement 声明
     * @param args      参数
     * @param names     占位符参数名
     * @throws SQLException ex
     */
    public static void registerParams(CallableStatement statement, Map<String, Param> args, List<String> names) throws SQLException {
        if (args != null && !args.isEmpty()) {
            for (int i = 0; i < names.size(); i++) {
                if (args.containsKey(names.get(i))) {
                    int index = i + 1;
                    Param param = args.get(names.get(i));
                    if (param.getParamMode() == ParamMode.IN) {
                        statement.setObject(index, SqlUtil.unWrapValue(param.getValue()));
                        continue;
                    }
                    if (param.getParamMode() == ParamMode.OUT) {
                        statement.registerOutParameter(index, param.getType().getTypeNumber());
                        continue;
                    }
                    if (param.getParamMode() == ParamMode.IN_OUT) {
                        statement.setObject(index, SqlUtil.unWrapValue(param.getValue()));
                        statement.registerOutParameter(index, param.getType().getTypeNumber());
                    }
                }
            }
        }
    }
}
