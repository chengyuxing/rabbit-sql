package com.github.chengyuxing.sql.transaction;

import com.github.chengyuxing.sql.datasource.AbstractTransactionSyncManager;
import com.github.chengyuxing.sql.datasource.ConnectionHolder;
import com.github.chengyuxing.sql.datasource.DataSourceUtils;
import com.github.chengyuxing.sql.exceptions.TransactionException;

import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * Transaction util.
 */
public final class Tx {

    /**
     * Begin transaction.
     *
     * @param definition transaction definition
     * @see #using(Runnable, Definition)
     * @see #using(Supplier, Definition)
     */
    public static void begin(Definition definition) {
        AbstractTransactionSyncManager.initTransaction(definition);
        if (!AbstractTransactionSyncManager.isSynchronizationActive()) {
            AbstractTransactionSyncManager.initSynchronization();
        }
    }

    /**
     * Begin transaction.
     *
     * @see #using(Runnable)
     * @see #using(Supplier)
     */
    public static void begin() {
        begin(Definition.defaultDefinition());
    }

    /**
     * Commit transaction.
     *
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
     * Rollback transaction.
     *
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
     * Begin and auto commit/rollback transaction.
     *
     * @param runnable   runnable
     * @param definition transaction definition
     * @see #begin(Definition)
     * @see #commit()
     * @see #rollback()
     */
    public static void using(Runnable runnable, Definition definition) {
        try {
            begin(definition);
            runnable.run();
            commit();
        } catch (Throwable e) {
            rollback();
            throw e;
        }
    }

    /**
     * Begin and auto commit/rollback transaction.
     *
     * @param supplier   supplier
     * @param definition transaction definition
     * @param <T>        result type
     * @return result
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
            return result;
        } catch (Throwable e) {
            rollback();
            throw e;
        }
    }

    /**
     * Begin and auto commit/rollback transaction.
     *
     * @param runnable runnable
     * @see #begin()
     * @see #commit()
     * @see #rollback()
     */
    public static void using(Runnable runnable) {
        using(runnable, Definition.defaultDefinition());
    }

    /**
     * Begin and auto commit/rollback transaction.
     *
     * @param supplier supplier
     * @param <T>      result type
     * @return result
     * @see #begin()
     * @see #commit()
     * @see #rollback()
     */
    public static <T> T using(Supplier<T> supplier) {
        return using(supplier, Definition.defaultDefinition());
    }

    private static void releaseTransaction() {
        if (AbstractTransactionSyncManager.isSynchronizationActive()) {
            AbstractTransactionSyncManager.getSynchronizations().forEach(DataSourceUtils.TransactionSynchronization::afterCompletion);
        }
        AbstractTransactionSyncManager.clear();
    }

    private static void commitTransaction() {
        AbstractTransactionSyncManager.getSynchronizations().forEach(s -> {
            ConnectionHolder holder = s.getConnectionHolder();
            if (holder.hasConnection()) {
                try {
                    holder.getConnection().commit();
                } catch (SQLException e) {
                    throw new TransactionException("Transaction commit failed.", e);
                }
            }
        });
    }

    private static void rollbackTransaction() {
        AbstractTransactionSyncManager.getSynchronizations().forEach(s -> {
            ConnectionHolder holder = s.getConnectionHolder();
            if (holder.hasConnection()) {
                try {
                    holder.getConnection().rollback();
                } catch (SQLException e) {
                    throw new TransactionException("Transaction rollback failed.", e);
                }
            }
        });
    }
}
