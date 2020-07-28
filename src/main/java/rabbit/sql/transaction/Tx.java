package rabbit.sql.transaction;

import rabbit.sql.datasource.AbstractTransactionSyncManager;
import rabbit.sql.datasource.ConnectionHolder;
import rabbit.sql.datasource.DataSourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * 同步事务管理器
 */
public final class Tx {
    private final static Logger log = LoggerFactory.getLogger(Tx.class);

    /**
     * 开始事务
     *
     * @param definition 事务定义
     */
    public static void begin(Definition definition) {
        //记住事务的定义，以在后续其他操作中沿用此事务的定义
        AbstractTransactionSyncManager.rememberTransactionDef(definition);
        if (!AbstractTransactionSyncManager.isSynchronizationActive()) {
            AbstractTransactionSyncManager.initSynchronization();
        }
    }

    /**
     * 开始事务
     */
    public static void begin() {
        begin(Definition.defaultDefinition());
    }

    /**
     * 提交事务
     */
    public static void commit() {
        try {
            commitTransaction();
        } finally {
            releaseTransaction();
        }
    }

    /**
     * 回滚事务
     */
    public static void rollback() {
        try {
            rollbackTransaction();
        } finally {
            releaseTransaction();
        }
    }

    /**
     * 自动提交/回滚事务
     *
     * @param runnable   sql执行操作
     * @param definition 事务定义
     */
    public static void using(Runnable runnable, Definition definition) {
        try {
            begin(definition);
            runnable.run();
            commit();
        } catch (Exception e) {
            rollback();
            log.error("transaction will rollback.");
            e.printStackTrace();
        }
    }

    /**
     * 自动提交/回滚事务
     *
     * @param supplier   sql执行操作
     * @param definition 事务定义
     * @param <T>        类型参数
     * @return 回调结果
     */
    public static <T> T using(Supplier<T> supplier, Definition definition) {
        T result = null;
        try {
            begin(definition);
            result = supplier.get();
            commit();
        } catch (Exception e) {
            rollback();
            log.error("transaction will rollback.");
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 自动提交/回滚事务
     *
     * @param runnable sql执行操作
     */
    public static void using(Runnable runnable) {
        using(runnable, Definition.defaultDefinition());
    }

    /**
     * 自动提交/回滚事务
     *
     * @param supplier sql执行操作
     * @param <T>      类型参数
     * @return 回调结果
     */
    public static <T> T using(Supplier<T> supplier) {
        return using(supplier, Definition.defaultDefinition());
    }

    /**
     * 释放单前连接对象并清除事务资源
     */
    private static void releaseTransaction() {
        if (AbstractTransactionSyncManager.isSynchronizationActive()) {
            AbstractTransactionSyncManager.getSynchronizations().forEach(DataSourceUtil.TransactionSynchronization::afterCompletion);
        }
        //清除事务定义资源
        AbstractTransactionSyncManager.clear();
    }

    /**
     * 同步提交事务
     */
    private static void commitTransaction() {
        log.info("commit transaction!");
        AbstractTransactionSyncManager.getSynchronizations().forEach(s -> {
            ConnectionHolder holder = s.getConnectionHolder();
            if (holder.hasConnection()) {
                try {
                    holder.getConnection().commit();
                } catch (SQLException e) {
                    log.error("transaction commit failed:{}", e.toString());
                }
            }
        });
    }

    /**
     * 同步回滚事务
     */
    private static void rollbackTransaction() {
        log.info("rollback transaction!");
        AbstractTransactionSyncManager.getSynchronizations().forEach(s -> {
            ConnectionHolder holder = s.getConnectionHolder();
            if (holder.hasConnection()) {
                try {
                    holder.getConnection().rollback();
                } catch (SQLException e) {
                    log.error("transaction rollback failed:{}", e.toString());
                }
            }
        });
    }
}
