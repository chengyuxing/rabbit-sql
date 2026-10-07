package tests;

import com.github.chengyuxing.sql.datasource.AbstractTransactionSyncManager;
import com.github.chengyuxing.sql.datasource.DataSourceUtils;
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

    private static class RecordingConnection {
        int commits;
        int rollbacks;
        int closes;
        String failingMethod;
        final SQLException setupFailure = new SQLException("setup failed");
        SQLException closeFailure;
        final Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    String name = method.getName();
                    if (name.equals(failingMethod)) {
                        throw setupFailure;
                    }
                    switch (name) {
                        case "commit": commits++; break;
                        case "rollback": rollbacks++; break;
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
