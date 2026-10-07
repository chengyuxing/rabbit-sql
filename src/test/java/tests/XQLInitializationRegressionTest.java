package tests;

import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.exceptions.XQLParseException;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class XQLInitializationRegressionTest {
    @Test
    public void failedInitialLoadCanBeRetried() {
        XQLFileManager manager = new XQLFileManager();
        manager.add("example", "missing-regression-resource.xql");
        assertThrows(XQLParseException.class, manager::init);
        assertFalse(manager.isInitialized());
        assertFalse(manager.isLoading());
        assertTrue(manager.getResources().isEmpty());

        manager.remove("example");
        manager.add("example", "dynamic-sql-example/deep_nest.xql");
        manager.init();
        assertTrue(manager.isInitialized());
        assertFalse(manager.isLoading());
        assertNotNull(manager.getResource("example"));
    }

    @Test
    public void failedPipeInitializationDoesNotPublishResources() {
        XQLFileManager manager = new XQLFileManager();
        manager.add("example", "dynamic-sql-example/deep_nest.xql");
        manager.setPipes(Collections.singletonMap("bad", "missing.regression.Pipe"));
        assertThrows(XQLParseException.class, manager::init);
        assertFalse(manager.isInitialized());
        assertFalse(manager.isLoading());
        assertTrue(manager.getResources().isEmpty());
        assertTrue(manager.getPipeInstances().isEmpty());
    }

    @Test
    public void failedReloadPreservesPreviouslyLoadedState() {
        XQLFileManager manager = new XQLFileManager();
        manager.add("example", "dynamic-sql-example/deep_nest.xql");
        manager.init();
        Map<String, XQLFileManager.Resource> resources = manager.getResources();
        Map<?, ?> pipes = manager.getPipeInstances();

        manager.add("another", "dynamic-sql-example/choose.xql");
        manager.setPipes(Collections.singletonMap("bad", "missing.regression.Pipe"));
        assertThrows(XQLParseException.class, manager::init);
        assertTrue(manager.isInitialized());
        assertFalse(manager.isLoading());
        assertSame(resources, manager.getResources());
        assertSame(pipes, manager.getPipeInstances());
        assertNull(manager.getResource("another"));
    }
}
