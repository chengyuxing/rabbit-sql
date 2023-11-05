package com.github.chengyuxing.sql.datasource;

import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static com.github.chengyuxing.sql.datasource.AbstractTransactionSyncManager.*;

/**
 * Datasource util.
 */
public abstract class DataSourceUtil {
    /**
     * Get 1 connection.
     *
     * @param dataSource datasource
     * @return connection
     * @throws SQLException if datasource states error
     */
    public static Connection getConnection(DataSource dataSource) throws SQLException {
        return doGetConnection(dataSource);
    }

    private static Connection doGetConnection(DataSource dataSource) throws SQLException {
        ConnectionHolder connectionHolder = getResource(dataSource);
        // It means current connection not released, maybe in transaction, reuse this connection for same state.
        if (connectionHolder != null && (connectionHolder.hasConnection() || connectionHolder.isSyncWithTransaction())) {
            connectionHolder.requested();
            if (!connectionHolder.hasConnection()) {
                connectionHolder.setConnection(fetchConnection(dataSource));
            }
            return connectionHolder.getConnection();
        }
        // Get a new connection, maybe from another datasource.
        Connection connection = fetchConnection(dataSource);
        // If transaction is active, bind new connection to transaction in current thread.
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
     * Close connection.
     *
     * @param connection connection
     * @throws SQLException if connection states error
     */
    private static void doCloseConnection(Connection connection) throws SQLException {
        connection.close();
    }

    /**
     * Close connection or release connection reference.
     *
     * @param connection connection
     * @param dataSource datasource
     */
    public static void releaseConnection(Connection connection, DataSource dataSource) {
        try {
            doReleaseConnection(connection, dataSource);
        } catch (SQLException e) {
            throw new UncheckedSqlException("Couldn't close JDBC connection:{}", e);
        }
    }

    /**
     * Close connection or release connection reference.
     *
     * @param connection connection
     * @param dataSource datasource
     * @throws SQLException if connection states error
     */
    private static void doReleaseConnection(Connection connection, DataSource dataSource) throws SQLException {
        if (connection == null) {
            return;
        }
        if (dataSource != null) {
            ConnectionHolder holder = getResource(dataSource);
            if (holder != null && connectionEquals(holder, connection)) {
                // Connection in transaction now, do not close, just release reference count.
                holder.released();
                return;
            }
        }
        doCloseConnection(connection);
    }

    /**
     * Check connection is in transaction or not.
     *
     * @param connection connection
     * @param dataSource datasource
     * @return connection is in transaction or not
     */
    public static boolean isConnectionTransactional(Connection connection, DataSource dataSource) {
        if (dataSource == null) {
            return false;
        }
        ConnectionHolder conHolder = getResource(dataSource);
        return (conHolder != null && connectionEquals(conHolder, connection));
    }

    /**
     * Check held connection is passed-in or not.
     *
     * @param holder       connection holder
     * @param passedInConn passed in connection
     * @return true if equal or false
     */
    private static boolean connectionEquals(ConnectionHolder holder, Connection passedInConn) {
        if (!holder.hasConnection()) {
            return false;
        }
        Connection heldConn = holder.getConnection();
        return (heldConn == passedInConn || heldConn.equals(passedInConn));
    }

    /**
     * Get a new connection.
     *
     * @param dataSource datasource
     * @return new connection
     * @throws SQLException              if connection states error
     * @throws ConnectionStatusException if datasource state error
     */
    private static Connection fetchConnection(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        if (connection == null) {
            throw new ConnectionStatusException("DataSource returned null from DataSource: " + dataSource);
        }
        return connection;
    }

    /**
     * Transaction synchronization object.
     */
    public static class TransactionSynchronization {
        private final ConnectionHolder connectionHolder;
        private final DataSource dataSource;

        public TransactionSynchronization(ConnectionHolder connectionHolder, DataSource dataSource) {
            this.connectionHolder = connectionHolder;
            this.dataSource = dataSource;
        }

        public ConnectionHolder getConnectionHolder() {
            return connectionHolder;
        }

        /**
         * Close connection and clear connection state after transaction complete.
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
