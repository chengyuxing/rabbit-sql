package com.github.chengyuxing.sql.datasource;

import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static com.github.chengyuxing.sql.datasource.AbstractTransactionSyncManager.*;

/**
 * 数据源工具
 */
public abstract class DataSourceUtil {
    /**
     * 获取一个连接对象
     *
     * @param dataSource 数据源
     * @return 连接
     * @throws SQLException ex
     */
    public static Connection getConnection(DataSource dataSource) throws SQLException {
        return doGetConnection(dataSource);
    }

    private static Connection doGetConnection(DataSource dataSource) throws SQLException {
        ConnectionHolder connectionHolder = getResource(dataSource);
        // 当执行此分之时，说明当前是在一个事务中，事务还没有结束，新的请求将沿用带有事务的此连接对象
        if (connectionHolder != null && (connectionHolder.hasConnection() || connectionHolder.isSyncWithTransaction())) {
            connectionHolder.requested();
            if (!connectionHolder.hasConnection()) {
                connectionHolder.setConnection(fetchConnection(dataSource));
            }
            return connectionHolder.getConnection();
        }
        // 一般情况这是首次获取一个不绑定线程的连接对象
        // 或者是从另一个数据源首次获取连接对象
        Connection connection = fetchConnection(dataSource);
        // 如果当前有正在活动的事务，则将连接绑定到当前的事务中，否则直接返回一个不绑定事务的连接
        if (isTransactionActive()) {
            ConnectionHolder holderToUse = connectionHolder;
            if (holderToUse == null) {
                holderToUse = new ConnectionHolder(connection);
            } else {
                holderToUse.setConnection(connection);
            }
            holderToUse.requested();
            connection.setAutoCommit(false);
            //noinspection MagicConstant
            connection.setTransactionIsolation(getCurrentTransactionIsolationLevel());
            connection.setReadOnly(isCurrentTransactionReadOnly());
            registerSynchronization(new TransactionSynchronization(holderToUse, dataSource));
            holderToUse.setSyncWithTransaction(true);
            if (holderToUse != connectionHolder) {
                bindResource(dataSource, holderToUse);
            }
        }
        return connection;
    }

    /**
     * 关闭连接对象
     *
     * @param connection 连接对象
     * @throws SQLException ex
     */
    private static void doCloseConnection(Connection connection) throws SQLException {
        connection.close();
    }

    /**
     * 如果有必要，则释放连接对象
     *
     * @param connection 连接对象
     * @param dataSource 数据源
     */
    public static void releaseConnection(Connection connection, DataSource dataSource) {
        try {
            doReleaseConnection(connection, dataSource);
        } catch (SQLException e) {
            throw new UncheckedSqlException("Couldn't close JDBC connection:{}", e);
        }
    }

    /**
     * 如果事务结束或当前没有事务，则释放连接对象资源
     *
     * @param connection 连接对象
     * @param dataSource 数据源
     * @throws SQLException 释放连接对象失败
     */
    private static void doReleaseConnection(Connection connection, DataSource dataSource) throws SQLException {
        if (connection == null) {
            return;
        }
        if (dataSource != null) {
            ConnectionHolder holder = getResource(dataSource);
            if (holder != null && connectionEquals(holder, connection)) {
                // 此连接对象在事务中，不关闭，将引用计数减1
                holder.released();
                return;
            }
        }
        doCloseConnection(connection);
    }

    /**
     * 判断传入的连接是否在以绑定的事务上下文中
     *
     * @param con        连接对象
     * @param dataSource 数据源
     * @return 连接是否是事务的
     */
    public static boolean isConnectionTransactional(Connection con, DataSource dataSource) {
        if (dataSource == null) {
            return false;
        }
        ConnectionHolder conHolder = getResource(dataSource);
        return (conHolder != null && connectionEquals(conHolder, con));
    }

    /**
     * 比较连接对象句柄中的连接对象和当前传入的连接对象是否是同一个
     *
     * @param holder       事务绑定的连接对象
     * @param passedInConn 传入的连接对象
     * @return 是否相等
     */
    private static boolean connectionEquals(ConnectionHolder holder, Connection passedInConn) {
        if (!holder.hasConnection()) {
            return false;
        }
        Connection heldConn = holder.getConnection();
        return (heldConn == passedInConn || heldConn.equals(passedInConn));
    }

    /**
     * 抓取一个新的连接对象
     *
     * @param dataSource 数据源
     * @return new connection
     * @throws SQLException              ex
     * @throws ConnectionStatusException 如果连接对象为null
     */
    private static Connection fetchConnection(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        if (connection == null) {
            throw new ConnectionStatusException("DataSource returned null from DataSource: " + dataSource);
        }
        return connection;
    }

    /**
     * 事务同步
     */
    public static class TransactionSynchronization {
        private final ConnectionHolder connectionHolder;
        private final DataSource dataSource;

        public TransactionSynchronization(ConnectionHolder connectionHolder, DataSource dataSource) {
            this.connectionHolder = connectionHolder;
            this.dataSource = dataSource;
        }

        /**
         * 获取连接对象
         *
         * @return 连接对象
         */
        public ConnectionHolder getConnectionHolder() {
            return connectionHolder;
        }

        /**
         * 事务完成后执行此操作，关闭连接对象并解除资源绑定
         */
        public void afterCompletion() {
            unbindResource(dataSource);
            if (connectionHolder.hasConnection()) {
                releaseConnection(connectionHolder.getConnection(), null);
                connectionHolder.setConnection(null);
            }
            connectionHolder.clear();
        }
    }
}
