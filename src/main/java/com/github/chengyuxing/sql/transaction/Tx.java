package com.github.chengyuxing.sql.transaction;

import com.github.chengyuxing.sql.datasource.AbstractTransactionSyncManager;
import com.github.chengyuxing.sql.datasource.ConnectionHolder;
import com.github.chengyuxing.sql.datasource.DataSourceUtils;
import com.github.chengyuxing.sql.exceptions.TransactionException;

import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Transaction util.
 */
public final class Tx {

    /**
     * Begin transaction.
     *
     * @param definition transaction definition
     * @throws IllegalStateException if a transaction is already active in the current thread
     * @see #using(Runnable, Definition)
     * @see #using(Supplier, Definition)
     */
    public static void begin(Definition definition) {
        Objects.requireNonNull(definition, "Transaction definition must not be null.");
        if (AbstractTransactionSyncManager.isTransactionActive()) {
            throw new IllegalStateException("Nested transactions are not supported.");
        }
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
        completeTransaction(true);
    }

    /**
     * Rollback transaction.
     *
     * @see #begin(Definition)
     * @see #begin()
     */
    public static void rollback() {
        completeTransaction(false);
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
        using(() -> {
            runnable.run();
            return null;
        }, definition);
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
        begin(definition);
        try {
            result = supplier.get();
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable rollbackError) {
                if (rollbackError != e) {
                    e.addSuppressed(rollbackError);
                }
            }
            throw e;
        }
        commit();
        return result;
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

    private static void completeTransaction(boolean commit) {
        Throwable failure = null;
        try {
            if (commit) {
                commitTransaction();
            } else {
                rollbackTransaction();
            }
        } catch (RuntimeException | Error e) {
            failure = e;
            if (commit) {
                try {
                    rollbackTransaction();
                } catch (RuntimeException | Error rollbackError) {
                    if (rollbackError != e) e.addSuppressed(rollbackError);
                }
            }
            throw e;
        } finally {
            try {
                releaseTransaction();
            } catch (RuntimeException | Error cleanupError) {
                if (failure == null) throw cleanupError;
                if (cleanupError != failure) failure.addSuppressed(cleanupError);
            }
        }
    }

    private static void releaseTransaction() {
        Throwable failure = null;
        try {
            for (DataSourceUtils.TransactionSynchronization synchronization : AbstractTransactionSyncManager.getSynchronizations()) {
                try {
                    synchronization.afterCompletion();
                } catch (RuntimeException | Error e) {
                    if (failure == null) failure = e;
                    else if (e != failure) failure.addSuppressed(e);
                }
            }
        } finally {
            AbstractTransactionSyncManager.clear();
        }
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) throw (RuntimeException) failure;
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
        TransactionException failure = null;
        for (DataSourceUtils.TransactionSynchronization s : AbstractTransactionSyncManager.getSynchronizations()) {
            ConnectionHolder holder = s.getConnectionHolder();
            if (holder.hasConnection()) {
                try {
                    holder.getConnection().rollback();
                } catch (SQLException e) {
                    if (failure == null) failure = new TransactionException("Transaction rollback failed.", e);
                    else failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) throw failure;
    }
}
