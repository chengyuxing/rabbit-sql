package tests;

import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.exceptions.XQLParseException;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class XQLInitializationRegressionTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void reloadsConstantsWithoutRequiringAFileModification() throws Exception {
        File file = folder.newFile("constants.xql");
        Files.write(file.toPath(), "/* [q] */\nselect ${constant};\n".getBytes(StandardCharsets.UTF_8));
        XQLFileManager manager = new XQLFileManager();
        manager.add("x", file.toURI().toString());
        manager.setConstants(Collections.singletonMap("constant", "1"));
        manager.init();
        XQLFileManager.Resource resource = manager.getResource("x");
        manager.init();
        assertSame(resource, manager.getResource("x"));
        manager.setConstants(Collections.singletonMap("constant", "2"));
        manager.init();
        assertEquals("select 2", manager.get("x.q"));
        assertNotSame(resource, manager.getResource("x"));
    }

    @Test
    public void samePipeNameCanSwitchImplementationClasses() {
        XQLFileManager manager = new XQLFileManager();
        manager.setPipes(Collections.singletonMap("p", "com.github.chengyuxing.common.script.pipe.builtin.Upper"));
        manager.init();
        Object old = manager.getPipeInstances().get("p");
        manager.init();
        assertSame(old, manager.getPipeInstances().get("p"));
        manager.setPipes(Collections.singletonMap("p", "com.github.chengyuxing.common.script.pipe.builtin.Lower"));
        manager.init();
        assertEquals("Lower", manager.getPipeInstances().get("p").getClass().getSimpleName());
        assertNotSame(old, manager.getPipeInstances().get("p"));
    }

    @Test
    public void failedReloadDoesNotAdvanceTheConfigurationSnapshot() throws Exception {
        File file = folder.newFile("retry.xql");
        Files.write(file.toPath(), "/* [q] */\nselect ${constant};\n".getBytes(StandardCharsets.UTF_8));
        XQLFileManager manager = new XQLFileManager();
        manager.add("x", file.toURI().toString());
        manager.setConstants(Collections.singletonMap("constant", "1"));
        manager.init();
        manager.setConstants(Collections.singletonMap("constant", "2"));
        manager.setPipes(Collections.singletonMap("bad", "missing.regression.Pipe"));
        assertThrows(XQLParseException.class, manager::init);
        assertEquals("select 1", manager.get("x.q"));
        manager.setPipes(Collections.emptyMap());
        manager.init();
        assertEquals("select 2", manager.get("x.q"));
    }

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
