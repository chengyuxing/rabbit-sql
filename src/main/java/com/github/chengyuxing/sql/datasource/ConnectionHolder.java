package com.github.chengyuxing.sql.datasource;

import java.sql.Connection;

public class ConnectionHolder {

    private Connection currentConnection;
    private boolean syncWithTransaction = false;
    private int refCount = 0;

    public ConnectionHolder(Connection connection) {
        this.currentConnection = connection;
    }

    public boolean hasConnection() {
        return currentConnection != null;
    }

    public void setConnection(Connection connection) {
        currentConnection = connection;
    }

    public Connection getConnection() {
        if (currentConnection != null) {
            return currentConnection;
        }
        return null;
    }

    public void setSyncWithTransaction(boolean syncWithTransaction) {
        this.syncWithTransaction = syncWithTransaction;
    }

    public boolean isSyncWithTransaction() {
        return syncWithTransaction;
    }

    /**
     * Check any connection is active.
     *
     * @return reference count &gt; 0 or not
     */
    public boolean isOpen() {
        return refCount > 0;
    }

    /**
     * Request current connection.
     */
    public void requested() {
        refCount++;
    }

    /**
     * Release current connection.
     */
    public void released() {
        refCount--;
    }

    /**
     * Clear connection reference count.
     */
    public void clear() {
        refCount = 0;
    }
}
