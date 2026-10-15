package tests;

import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLInvocationHandler;
import com.github.chengyuxing.sql.annotation.XQLMapper;
import com.github.chengyuxing.sql.util.XQLMapperUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.*;

public class MapperRegressionTest {
    @XQLMapper("example")
    public interface Mapper {
        default int findValue() { return 42; }
        java.util.List<?> findValues();
    }

    @Test
    public void mapperInvocationHonorsCustomReturnGenericTypeResolution() throws IllegalAccessException {
        java.util.concurrent.atomic.AtomicBoolean resolved = new java.util.concurrent.atomic.AtomicBoolean();
        IllegalStateException stopBeforeDatabase = new IllegalStateException("Stop before accessing the database");
        Mapper mapper = XQLMapperUtils.getProxyInstance(Mapper.class, new XQLInvocationHandler() {
            @Override
            protected Class<?> getReturnGenericType(java.lang.reflect.Method method) throws ClassNotFoundException {
                assertEquals("findValues", method.getName());
                resolved.set(true);
                // The default resolver rejects this wildcard; an override can choose a concrete type.
                return String.class;
            }

            @Override
            protected @NotNull BakiDao baki() {
                assertTrue(resolved.get());
                throw stopBeforeDatabase;
            }
        });
        assertSame(stopBeforeDatabase, assertThrows(IllegalStateException.class, mapper::findValues));
        assertTrue(resolved.get());
    }

    private Mapper mapper() throws IllegalAccessException {
        return XQLMapperUtils.getProxyInstance(Mapper.class, new XQLInvocationHandler() {
            @Override
            protected @NotNull BakiDao baki() {
                throw new AssertionError("Object and default methods must not access the database");
            }
        });
    }

    @Test
    public void objectMethodsUseProxyIdentityWithoutDatabaseAccess() throws IllegalAccessException {
        Mapper first = mapper();
        Mapper second = mapper();
        assertTrue(first.equals(first));
        assertFalse(first.equals(second));
        assertFalse(first.equals(null));
        assertFalse(first.equals(new Object()));
        assertEquals(System.identityHashCode(first), first.hashCode());
        assertEquals("XQLMapper[" + Mapper.class.getName() + "]@" + Integer.toHexString(first.hashCode()), first.toString());
    }

    @Test
    public void defaultMethodsAreExplicitlyRejected() throws IllegalAccessException {
        UnsupportedOperationException error = assertThrows(UnsupportedOperationException.class, mapper()::findValue);
        assertTrue(error.getMessage().contains("Default mapper methods are not supported"));
    }
}
