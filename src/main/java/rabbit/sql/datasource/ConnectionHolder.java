package rabbit.sql.datasource;

import java.sql.Connection;

/**
 * 连接对象持有者
 */
public class ConnectionHolder {

    private Connection currentConnection;
    private boolean syncWithTransaction = false;
    private int refCount = 0;

    /**
     * 构造函数
     *
     * @param connection 连接对象
     */
    public ConnectionHolder(Connection connection) {
        this.currentConnection = connection;
    }

    /**
     * 是否有连接对象
     *
     * @return 是否有
     */
    public boolean hasConnection() {
        return currentConnection != null;
    }

    /**
     * 设置连接对象
     *
     * @param connection 连接对象
     */
    public void setConnection(Connection connection) {
        currentConnection = connection;
    }

    /**
     * 获取当前持有的连接对象
     *
     * @return 连接对象
     */
    public Connection getConnection() {
        if (currentConnection != null) {
            return currentConnection;
        }
        return null;
    }

    /**
     * 设置事务同步
     *
     * @param syncWithTransaction 是否事务同步
     */
    public void setSyncWithTransaction(boolean syncWithTransaction) {
        this.syncWithTransaction = syncWithTransaction;
    }

    /**
     * 当前是否开启事务同步
     *
     * @return 是否事务同步
     */
    public boolean isSyncWithTransaction() {
        return syncWithTransaction;
    }

    /**
     * 连接对象是否打开
     *
     * @return 打开状态
     */
    public boolean isOpen() {
        return refCount > 0;
    }

    /**
     * 当前连接引用被请求
     */
    public void requested() {
        refCount++;
    }

    /**
     * 当前连接引用释放
     */
    public void released() {
        refCount--;
    }

    /**
     * 清理当前连接的引用
     */
    public void clear() {
        refCount = 0;
    }
}
