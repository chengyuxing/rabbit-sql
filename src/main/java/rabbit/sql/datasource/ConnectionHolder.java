package rabbit.sql.datasource;

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

    public boolean isOpen() {
        return refCount > 0;
    }

    public void requested() {
        refCount++;
    }

    public void released() {
        refCount--;
    }

    public void clear() {
        refCount = 0;
    }
}
