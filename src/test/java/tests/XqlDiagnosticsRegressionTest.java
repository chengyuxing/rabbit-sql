package tests;

import com.github.chengyuxing.common.script.exception.CheckViolationException;
import com.github.chengyuxing.common.script.exception.GuardViolationException;
import com.github.chengyuxing.common.script.exception.LexerException;
import com.github.chengyuxing.common.script.exception.PipeNotFoundException;
import com.github.chengyuxing.common.script.exception.ScriptSyntaxException;
import com.github.chengyuxing.common.script.pipe.IPipe;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.exceptions.XQLParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.Assert.*;

public class XqlDiagnosticsRegressionTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void syntaxErrorShowsFileQualifiedSqlNameAndTokenPosition() throws Exception {
        XQLFileManager manager = manager("/* [q] */\nselect 1\n-- #if :id and :other\n-- #fi;");
        XQLParseException ex = assertThrows(XQLParseException.class, manager::init);
        assertContext(manager, ex);
        assertTrue(ex.getCause() instanceof ScriptSyntaxException);
        assertTrue(ex.getMessage().contains(ex.getCause().getMessage()));
        assertTrue(ex.getMessage().contains("and"));
        assertTrue(ex.getMessage().contains("SQL fragment position"));
    }

    @Test
    public void unterminatedStringShowsItsSqlFragmentLine() throws Exception {
        XQLFileManager manager = manager("/* [q] */\nselect 1\n-- #if :id == 'unterminated\n-- #fi;");
        XQLParseException ex = assertThrows(XQLParseException.class, manager::init);
        assertContext(manager, ex);
        assertTrue(ex.getCause() instanceof LexerException);
        assertTrue(ex.getCause().getCause() instanceof LexerException);
        assertTrue(ex.getMessage().contains("Script line 2"));
        assertTrue(ex.getMessage().contains("Unterminated string literal"));
    }

    @Test
    public void expandedConstantsKeepFileAndSqlContextWhenRecompiled() throws Exception {
        XQLFileManager manager = manager("/* [q] */\nselect 1\n${condition}\nselect 2;");
        manager.setConstants(Collections.singletonMap("condition", "-- #if :id and :other\n-- #fi"));
        XQLParseException ex = assertThrows(XQLParseException.class, manager::init);
        assertContext(manager, ex);
        assertTrue(ex.getCause() instanceof ScriptSyntaxException);
    }

    @Test
    public void missingPipeShowsTheOwningSqlAndKeepsItsCause() throws Exception {
        XQLFileManager manager = manager("/* [q] */\n-- #var v = :id | missingPipe\nselect :v;");
        manager.init();
        XQLParseException ex = assertThrows(XQLParseException.class,
                () -> manager.get("probe.q", Collections.singletonMap("id", 1)));
        assertContext(manager, ex);
        assertTrue(ex.getCause() instanceof PipeNotFoundException);
        assertTrue(ex.getMessage().contains("missingPipe"));
        assertTrue(ex.getMessage().contains("evaluation error"));
    }

    @Test
    public void customPipeFailureKeepsTheOriginalExceptionInstance() throws Exception {
        XQLFileManager manager = manager("/* [q] */\n-- #var v = :id | explode\nselect :v;");
        manager.setPipes(Collections.singletonMap("explode", ExplodingPipe.class.getName()));
        manager.init();
        XQLParseException ex = assertThrows(XQLParseException.class,
                () -> manager.get("probe.q", Collections.singletonMap("id", 1)));
        assertContext(manager, ex);
        assertSame(ExplodingPipe.FAILURE, ex.getCause());
        assertTrue(ex.getMessage().contains("pipe rejected"));
    }

    @Test
    public void checkAndGuardKeepTheirBusinessExceptionTypesAndMessages() throws Exception {
        XQLFileManager check = manager("/* [q] */\n-- #check :id == blank throw 'id required'\nselect 1;");
        check.init();
        CheckViolationException checkFailure = assertThrows(CheckViolationException.class,
                () -> check.get("probe.q", Collections.emptyMap()));
        assertEquals("id required", checkFailure.getMessage());
        assertNull(checkFailure.getCause());

        XQLFileManager guard = manager("/* [q] */\nselect 1\n-- #guard :id != blank\nwhere id = :id\n-- #throw 'id required';");
        guard.init();
        GuardViolationException guardFailure = assertThrows(GuardViolationException.class,
                () -> guard.get("probe.q", Collections.emptyMap()));
        assertEquals("id required", guardFailure.getMessage());
        assertNull(guardFailure.getCause());
    }

    @Test
    public void publicSqlConstructorKeepsItsOriginalSyntaxExceptionType() {
        XQLFileManager manager = new XQLFileManager();
        assertThrows(ScriptSyntaxException.class, () -> manager.new Sql("-- #if :id and :other\n-- #fi"));
    }

    private XQLFileManager manager(String content) throws Exception {
        File file = folder.newFile(java.util.UUID.randomUUID() + ".xql");
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        XQLFileManager manager = new XQLFileManager();
        manager.add("probe", file.toURI().toString());
        return manager;
    }

    private void assertContext(XQLFileManager manager, XQLParseException ex) {
        assertTrue(ex.getMessage(), ex.getMessage().contains(manager.getFiles().get("probe")));
        assertTrue(ex.getMessage(), ex.getMessage().contains("probe.q"));
        assertNotNull(ex.getCause());
    }

    public static class ExplodingPipe implements IPipe<Object> {
        static final RuntimeException FAILURE = new IllegalArgumentException("pipe rejected");
        @Override public Object transform(Object value, Object... params) { throw FAILURE; }
    }
}
