package com.github.chengyuxing.sql.datasource;

import com.github.chengyuxing.sql.transaction.Definition;
import com.github.chengyuxing.common.NamedThreadLocal;
import com.github.chengyuxing.sql.datasource.DataSourceUtil.TransactionSynchronization;

import java.util.*;

/**
 * Abstract transaction sync manager.
 */
public abstract class AbstractTransactionSyncManager {
    /**
     * [datasource, connection]
     */
    public static final ThreadLocal<Map<Object, ConnectionHolder>> resources = new NamedThreadLocal<>("Connection resource");

    public static final ThreadLocal<Set<TransactionSynchronization>> synchronizations = new NamedThreadLocal<>("transaction synchronizations");

    public static final ThreadLocal<Boolean> currentTransactionReadOnly = new NamedThreadLocal<>("current connection's read only flag");

    public static final ThreadLocal<Integer> currentTransactionIsolationLevel = new NamedThreadLocal<>("transaction level");

    private static final ThreadLocal<Boolean> actualTransactionActive = new NamedThreadLocal<>("Actual transaction active");

    private static final ThreadLocal<String> currentTransactionName = new NamedThreadLocal<>("Current transaction name");

    public static ConnectionHolder getResource(Object key) {
        Map<Object, ConnectionHolder> map = resources.get();
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    public static void bindResource(Object key, ConnectionHolder value) {
        Map<Object, ConnectionHolder> map = resources.get();
        if (map == null) {
            map = new HashMap<>();
            resources.set(map);
        }
        map.put(key, value);
    }

    public static void unbindResource(Object key) {
        ConnectionHolder value = doUnbindResource(key);
        if (value == null) {
            throw new IllegalStateException("no value for key bound to thread: " + Thread.currentThread().getName());
        }
    }

    private static ConnectionHolder doUnbindResource(Object key) {
        Map<Object, ConnectionHolder> map = resources.get();
        if (map == null) {
            return null;
        }
        ConnectionHolder value = map.remove(key);
        if (map.isEmpty()) {
            resources.remove();
        }
        return value;
    }

    public static boolean isSynchronizationActive() {
        return synchronizations.get() != null;
    }

    public static void initSynchronization() {
        if (!isSynchronizationActive()) {
            synchronizations.set(new LinkedHashSet<>());
        }
    }

    public static void registerSynchronization(TransactionSynchronization synchronization) {
        if (!isSynchronizationActive()) {
            throw new IllegalStateException("transaction synchronization not active");
        }
        synchronizations.get().add(synchronization);
    }

    public static List<TransactionSynchronization> getSynchronizations() {
        Set<TransactionSynchronization> synchs = synchronizations.get();
        if (synchs == null || synchs.isEmpty())
            return Collections.emptyList();
        List<TransactionSynchronization> newSynchs = new ArrayList<>(synchs);
        return Collections.unmodifiableList(newSynchs);
    }

    public static void setCurrentTransactionName(String name) {
        currentTransactionName.set(name);
    }

    public static String getCurrentTransactionName() {
        return currentTransactionName.get();
    }

    public static void setCurrentTransactionReadOnly(boolean readOnly) {
        currentTransactionReadOnly.set(readOnly ? Boolean.TRUE : null);
    }

    public static boolean isCurrentTransactionReadOnly() {
        return currentTransactionReadOnly.get() != null;
    }

    public static void setTransactionActive(boolean active) {
        actualTransactionActive.set(active ? Boolean.TRUE : null);
    }

    public static boolean isTransactionActive() {
        return actualTransactionActive.get() != null;
    }

    public static void setCurrentTransactionIsolationLevel(Integer isolationLevel) {
        currentTransactionIsolationLevel.set(isolationLevel);
    }

    public static Integer getCurrentTransactionIsolationLevel() {
        return currentTransactionIsolationLevel.get();
    }

    public static void initTransaction(Definition definition) {
        setTransactionActive(true);
        setCurrentTransactionName(definition.getName());
        setCurrentTransactionReadOnly(definition.isReadOnly());
        setCurrentTransactionIsolationLevel(definition.getLevel());
    }

    public static void clear() {
        synchronizations.remove();
        currentTransactionIsolationLevel.remove();
        currentTransactionReadOnly.remove();
        actualTransactionActive.remove();
        currentTransactionName.remove();
    }
}
