package rabbit.sql.transaction;

import rabbit.sql.datasource.AbstractTransactionSyncManager;
import rabbit.sql.datasource.ConnectionHolder;
import rabbit.sql.datasource.DataSourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.sql.exceptions.SqlRuntimeException;
import rabbit.sql.exceptions.TransactionException;

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
     * @see #using(Runnable, Definition)
     * @see #using(Supplier, Definition)
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
     *
     * @see #using(Runnable)
     * @see #using(Supplier)
     */
    public static void begin() {
        begin(Definition.defaultDefinition());
    }

    /**
     * 提交事务
     *
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     * @see #begin(Definition)
     * @see #begin()
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
     *
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     * @see #begin(Definition)
     * @see #begin()
     */
    public static void rollback() {
        try {
            rollbackTransaction();
        } finally {
            releaseTransaction();
        }
    }

    /**
     * 新建一个事务自动提交/回滚事务
     *
     * @param runnable   sql执行操作
     * @param definition 事务定义
     * @throws SqlRuntimeException  如果在此事务中，sql执行错误则抛出异常
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     * @see #begin(Definition)
     * @see #commit()
     * @see #rollback()
     */
    public static void using(Runnable runnable, Definition definition) {
        try {
            begin(definition);
            runnable.run();
            commit();
        } catch (SqlRuntimeException e) {
            rollback();
            throw new SqlRuntimeException("transaction will rollback cause:", e);
        } catch (TransactionException e) {
            throw new TransactionException("transaction error:", e);
        }
    }

    /**
     * 新建一个事务自动提交/回滚事务
     *
     * @param supplier   sql执行操作
     * @param definition 事务定义
     * @param <T>        类型参数
     * @return 回调结果
     * @throws SqlRuntimeException  如果在此事务中，sql执行错误则抛出异常
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     * @see #begin(Definition)
     * @see #commit()
     * @see #rollback()
     */
    public static <T> T using(Supplier<T> supplier, Definition definition) {
        T result;
        try {
            begin(definition);
            result = supplier.get();
            commit();
        } catch (SqlRuntimeException e) {
            rollback();
            throw new SqlRuntimeException("transaction will rollback cause:", e);
        } catch (TransactionException e) {
            throw new TransactionException("transaction error:", e);
        }
        return result;
    }

    /**
     * 新建一个事务自动提交/回滚事务
     *
     * @param runnable sql执行操作
     * @throws SqlRuntimeException  如果在此事务中，sql执行错误则抛出异常
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     * @see #begin()
     * @see #commit()
     * @see #rollback()
     */
    public static void using(Runnable runnable) {
        using(runnable, Definition.defaultDefinition());
    }

    /**
     * 新建一个事务自动提交/回滚事务
     *
     * @param supplier sql执行操作
     * @param <T>      类型参数
     * @return 回调结果
     * @throws SqlRuntimeException  如果在此事务中，sql执行错误则抛出异常
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     * @see #begin()
     * @see #commit()
     * @see #rollback()
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
     *
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     */
    private static void commitTransaction() {
        log.info("commit transaction!");
        AbstractTransactionSyncManager.getSynchronizations().forEach(s -> {
            ConnectionHolder holder = s.getConnectionHolder();
            if (holder.hasConnection()) {
                try {
                    holder.getConnection().commit();
                } catch (SQLException e) {
                    throw new TransactionException("transaction commit failed: ", e);
                }
            }
        });
    }

    /**
     * 同步回滚事务
     *
     * @throws TransactionException 如果数据库错误，或者连接被关闭，或者数据库事务为自动提交
     */
    private static void rollbackTransaction() {
        log.info("rollback transaction!");
        AbstractTransactionSyncManager.getSynchronizations().forEach(s -> {
            ConnectionHolder holder = s.getConnectionHolder();
            if (holder.hasConnection()) {
                try {
                    holder.getConnection().rollback();
                } catch (SQLException e) {
                    throw new TransactionException("transaction rollback failed: ", e);
                }
            }
        });
    }
}
