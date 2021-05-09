package com.github.chengyuxing.sql.datasource;

import com.github.chengyuxing.sql.transaction.Definition;
import com.github.chengyuxing.common.NamedThreadLocal;
import com.github.chengyuxing.sql.datasource.DataSourceUtil.TransactionSynchronization;

import java.util.*;

/**
 * 抽象事务同步管理器
 */
public abstract class AbstractTransactionSyncManager {
    /**
     * 连接对象(数据源，连接对象)
     */
    public static final ThreadLocal<Map<Object, ConnectionHolder>> resources = new NamedThreadLocal<>("Connection resource");

    public static final ThreadLocal<Set<TransactionSynchronization>> synchronizations = new NamedThreadLocal<>("transaction synchronizations");

    public static final ThreadLocal<Boolean> currentTransactionReadOnly = new NamedThreadLocal<>("current connection's read only flag");

    public static final ThreadLocal<Integer> currentTransactionIsolationLevel = new NamedThreadLocal<>("transaction level");

    private static final ThreadLocal<Boolean> actualTransactionActive = new NamedThreadLocal<>("Actual transaction active");

    private static final ThreadLocal<String> currentTransactionName = new NamedThreadLocal<>("Current transaction name");

    /**
     * 获取资源
     *
     * @param key 键
     * @return 连接对象
     */
    public static ConnectionHolder getResource(Object key) {
        Map<Object, ConnectionHolder> map = resources.get();
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    /**
     * 绑定资源
     *
     * @param key   数据源
     * @param value 连接对象
     */
    public static void bindResource(Object key, ConnectionHolder value) {
        Map<Object, ConnectionHolder> map = resources.get();
        if (map == null) {
            map = new HashMap<>();
            resources.set(map);
        }
        map.put(key, value);
    }

    /**
     * 解绑资源
     *
     * @param key 数据源
     */
    public static void unbindResource(Object key) {
        ConnectionHolder value = doUnbindResource(key);
        if (value == null) {
            throw new IllegalStateException("no value for key bound to thread:" + Thread.currentThread().getName());
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

    /**
     * 事务同步是否活动
     *
     * @return 激活状态
     */
    public static boolean isSynchronizationActive() {
        return synchronizations.get() != null;
    }

    /**
     * 初始化事务同步
     */
    public static void initSynchronization() {
        if (!isSynchronizationActive()) {
            synchronizations.set(new LinkedHashSet<>());
        }
    }

    /**
     * 注册事务同步
     *
     * @param synchronization 事务同步
     */
    public static void registerSynchronization(TransactionSynchronization synchronization) {
        if (!isSynchronizationActive()) {
            throw new IllegalStateException("transaction synchronization not active");
        }
        synchronizations.get().add(synchronization);
    }

    /**
     * 获取全部同步的事务
     *
     * @return 事务对象序列
     */
    public static List<TransactionSynchronization> getSynchronizations() {
        Set<TransactionSynchronization> synchs = synchronizations.get();
        if (synchs == null || synchs.isEmpty())
            return Collections.emptyList();
        List<TransactionSynchronization> newSynchs = new ArrayList<>(synchs);
        return Collections.unmodifiableList(newSynchs);
    }

    /**
     * 设置当前事务的名字
     *
     * @param name 事务名
     */
    public static void setCurrentTransactionName(String name) {
        currentTransactionName.set(name);
    }

    /**
     * 获取事务同步名字
     *
     * @return 事务名
     */
    public static String getCurrentTransactionName() {
        return currentTransactionName.get();
    }

    /**
     * 设置事务是否只读
     *
     * @param readOnly 是否只读
     */
    public static void setCurrentTransactionReadOnly(boolean readOnly) {
        currentTransactionReadOnly.set(readOnly ? Boolean.TRUE : null);
    }

    /**
     * 获取当前事务是否只读
     *
     * @return 只读标记
     */
    public static boolean isCurrentTransactionReadOnly() {
        return currentTransactionReadOnly.get() != null;
    }

    /**
     * 设置当前事务是否活动
     *
     * @param active 是否活动
     */
    public static void setTransactionActive(boolean active) {
        actualTransactionActive.set(active ? Boolean.TRUE : null);
    }

    /**
     * 事务是否活动
     *
     * @return 是否活动
     */
    public static boolean isTransactionActive() {
        return actualTransactionActive.get() != null;
    }

    /**
     * 设置当前事务级别
     *
     * @param isolationLevel 事务级别
     */
    public static void setCurrentTransactionIsolationLevel(Integer isolationLevel) {
        currentTransactionIsolationLevel.set(isolationLevel);
    }

    /**
     * 获取当前事务级别
     *
     * @return 事务级别
     */
    public static Integer getCurrentTransactionIsolationLevel() {
        return currentTransactionIsolationLevel.get();
    }

    /**
     * 标记事务的定义激活事务
     *
     * @param definition 事务定义
     */
    public static void rememberTransactionDef(Definition definition) {
        setTransactionActive(true);
        setCurrentTransactionName(definition.getName());
        setCurrentTransactionReadOnly(definition.isReadOnly());
        setCurrentTransactionIsolationLevel(definition.getLevel());
    }

    /**
     * 清理事务资源
     */
    public static void clear() {
        synchronizations.remove();
        currentTransactionIsolationLevel.remove();
        currentTransactionReadOnly.remove();
        actualTransactionActive.remove();
        currentTransactionName.remove();
    }
}
