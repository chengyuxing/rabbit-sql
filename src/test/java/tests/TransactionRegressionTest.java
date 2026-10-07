package tests;

import com.github.chengyuxing.sql.datasource.AbstractTransactionSyncManager;
import com.github.chengyuxing.sql.datasource.DataSourceUtils;
import com.github.chengyuxing.sql.exceptions.TransactionException;
import com.github.chengyuxing.sql.transaction.Tx;
import org.junit.After;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class TransactionRegressionTest {
    @After
    public void clearTransaction() {
        if (AbstractTransactionSyncManager.isTransactionActive()) {
            Tx.rollback();
        }
    }

    @Test
    public void nestedBeginDoesNotChangeOuterTransaction() {
        Tx.begin();
        String name = AbstractTransactionSyncManager.getCurrentTransactionName();
        assertThrows(IllegalStateException.class, Tx::begin);
        assertTrue(AbstractTransactionSyncManager.isTransactionActive());
        assertEquals(name, AbstractTransactionSyncManager.getCurrentTransactionName());
    }

    @Test
    public void nestedRunnableDoesNotCommitOrRollBackOuterTransaction() throws SQLException {
        assertNestedTransactionRejected(false);
    }

    @Test
    public void nestedSupplierDoesNotCommitOrRollBackOuterTransaction() throws SQLException {
        assertNestedTransactionRejected(true);
    }

    private void assertNestedTransactionRejected(boolean supplier) throws SQLException {
        RecordingConnection recording = new RecordingConnection();
        Tx.begin();
        Connection connection = DataSourceUtils.getConnection(recording.dataSource);
        DataSourceUtils.releaseConnection(connection, recording.dataSource);
        if (supplier) {
            assertThrows(IllegalStateException.class, () -> Tx.using((Supplier<Integer>) () -> {
                fail("Nested transaction body must not run");
                return 1;
            }));
        } else {
            assertThrows(IllegalStateException.class, () -> Tx.using((Runnable) () -> fail("Nested transaction body must not run")));
        }
        assertTrue(AbstractTransactionSyncManager.isTransactionActive());
        assertEquals(0, recording.commits);
        assertEquals(0, recording.rollbacks);
        assertEquals(0, recording.closes);
        Tx.commit();
        assertEquals(1, recording.commits);
        assertEquals(1, recording.closes);
        assertFalse(AbstractTransactionSyncManager.isTransactionActive());
        assertNull(AbstractTransactionSyncManager.getResource(recording.dataSource));
    }

    @Test
    public void failedTransactionSetupClosesConnection() {
        for (String method : new String[]{"setAutoCommit", "setTransactionIsolation", "setReadOnly"}) {
            RecordingConnection recording = new RecordingConnection();
            recording.failingMethod = method;
            Tx.begin();
            SQLException error = assertThrows(SQLException.class, () -> DataSourceUtils.getConnection(recording.dataSource));
            assertSame(recording.setupFailure, error);
            assertEquals(method, 1, recording.closes);
            assertNull(AbstractTransactionSyncManager.getResource(recording.dataSource));
            assertTrue(AbstractTransactionSyncManager.getSynchronizations().isEmpty());
            Tx.rollback();
            assertEquals(1, recording.closes);
        }
    }

    @Test
    public void connectionCloseFailureDoesNotHideSetupFailure() {
        RecordingConnection recording = new RecordingConnection();
        recording.failingMethod = "setReadOnly";
        recording.closeFailure = new SQLException("close failed");
        Tx.begin();
        SQLException error = assertThrows(SQLException.class, () -> DataSourceUtils.getConnection(recording.dataSource));
        assertSame(recording.setupFailure, error);
        assertArrayEquals(new Throwable[]{recording.closeFailure}, error.getSuppressed());
    }

    @Test
    public void businessFailureSurvivesRollbackAndCleanupFailures() {
        RecordingConnection recording = new RecordingConnection();
        recording.rollbackFailure = new SQLException("rollback failed");
        recording.closeFailure = new SQLException("close failed");
        RuntimeException businessFailure = new RuntimeException("business failed");
        RuntimeException error = assertThrows(RuntimeException.class, () -> Tx.using((Runnable) () -> {
            acquire(recording);
            throw businessFailure;
        }));
        assertSame(businessFailure, error);
        assertEquals(1, error.getSuppressed().length);
        assertSame(recording.rollbackFailure, error.getSuppressed()[0].getCause());
        assertSame(recording.closeFailure, error.getSuppressed()[0].getSuppressed()[0].getCause());
        assertEquals(1, recording.rollbacks);
        assertEquals(1, recording.closes);
        assertTransactionCleared(recording);
    }

    @Test
    public void supplierFailurePreservesOriginalError() {
        RecordingConnection recording = new RecordingConnection();
        recording.rollbackFailure = new SQLException("rollback failed");
        AssertionError businessFailure = new AssertionError("business failed");
        AssertionError error = assertThrows(AssertionError.class, () -> Tx.using((Supplier<Integer>) () -> {
            acquire(recording);
            throw businessFailure;
        }));
        assertSame(businessFailure, error);
        assertSame(recording.rollbackFailure, error.getSuppressed()[0].getCause());
        assertTransactionCleared(recording);
    }

    @Test
    public void failedCommitAttemptsRollbackBeforeReleasingConnections() {
        RecordingConnection recording = new RecordingConnection();
        recording.commitFailure = new SQLException("commit failed");
        recording.rollbackFailure = new SQLException("rollback failed");
        recording.closeFailure = new SQLException("close failed");
        TransactionException error = assertThrows(TransactionException.class, () -> Tx.using((Runnable) () -> acquire(recording)));
        assertSame(recording.commitFailure, error.getCause());
        assertSame(recording.rollbackFailure, error.getSuppressed()[0].getCause());
        assertSame(recording.closeFailure, error.getSuppressed()[1].getCause());
        assertEquals(1, recording.commits);
        assertEquals(1, recording.rollbacks);
        assertEquals(1, recording.closes);
        assertTransactionCleared(recording);
    }

    @Test
    public void rollbackAndCleanupContinueAfterOneConnectionFails() {
        RecordingConnection first = new RecordingConnection();
        first.rollbackFailure = new SQLException("rollback failed");
        first.closeFailure = new SQLException("close failed");
        RecordingConnection second = new RecordingConnection();
        Tx.begin();
        acquire(first);
        acquire(second);
        TransactionException error = assertThrows(TransactionException.class, Tx::rollback);
        assertSame(first.rollbackFailure, error.getCause());
        assertSame(first.closeFailure, error.getSuppressed()[0].getCause());
        assertEquals(1, first.rollbacks);
        assertEquals(1, second.rollbacks);
        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertTransactionCleared(first);
        assertTransactionCleared(second);
    }

    @Test
    public void successfulSupplierCommitsAndReturnsResult() {
        RecordingConnection recording = new RecordingConnection();
        assertEquals(Integer.valueOf(42), Tx.using(() -> {
            acquire(recording);
            return 42;
        }));
        assertEquals(1, recording.commits);
        assertEquals(0, recording.rollbacks);
        assertEquals(1, recording.closes);
        assertTransactionCleared(recording);
    }

    @Test
    public void failedCleanupStillClosesOtherConnectionsAndClearsState() {
        RecordingConnection first = new RecordingConnection();
        first.closeFailure = new SQLException("first close failed");
        RecordingConnection second = new RecordingConnection();
        second.closeFailure = new SQLException("second close failed");
        Tx.begin();
        acquire(first);
        acquire(second);
        TransactionException error = assertThrows(TransactionException.class, Tx::commit);
        assertSame(first.closeFailure, error.getCause());
        assertSame(second.closeFailure, error.getSuppressed()[0].getCause());
        assertEquals(1, first.closes);
        assertEquals(1, second.closes);
        assertTransactionCleared(first);
        assertTransactionCleared(second);
    }

    @Test
    public void nullDefinitionDoesNotActivateTransaction() {
        assertThrows(NullPointerException.class, () -> Tx.begin(null));
        assertFalse(AbstractTransactionSyncManager.isTransactionActive());
        assertFalse(AbstractTransactionSyncManager.isSynchronizationActive());
    }

    private static void acquire(RecordingConnection recording) {
        try {
            DataSourceUtils.getConnection(recording.dataSource);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertTransactionCleared(RecordingConnection recording) {
        assertFalse(AbstractTransactionSyncManager.isTransactionActive());
        assertFalse(AbstractTransactionSyncManager.isSynchronizationActive());
        assertNull(AbstractTransactionSyncManager.getResource(recording.dataSource));
        assertNull(AbstractTransactionSyncManager.resources.get());
        assertNull(AbstractTransactionSyncManager.getCurrentTransactionName());
        assertNull(AbstractTransactionSyncManager.getCurrentTransactionIsolationLevel());
    }

    private static class RecordingConnection {
        int commits;
        int rollbacks;
        int closes;
        String failingMethod;
        final SQLException setupFailure = new SQLException("setup failed");
        SQLException closeFailure;
        SQLException commitFailure;
        SQLException rollbackFailure;
        final Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    String name = method.getName();
                    if (name.equals(failingMethod)) {
                        throw setupFailure;
                    }
                    switch (name) {
                        case "commit":
                            commits++;
                            if (commitFailure != null) throw commitFailure;
                            break;
                        case "rollback":
                            rollbacks++;
                            if (rollbackFailure != null) throw rollbackFailure;
                            break;
                        case "close":
                            closes++;
                            if (closeFailure != null) throw closeFailure;
                            break;
                        case "equals": return proxy == args[0];
                        case "hashCode": return System.identityHashCode(proxy);
                    }
                    return null;
                });
        final DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getConnection": return connection;
                        case "equals": return proxy == args[0];
                        case "hashCode": return System.identityHashCode(proxy);
                        default: return null;
                    }
                });
    }
}
