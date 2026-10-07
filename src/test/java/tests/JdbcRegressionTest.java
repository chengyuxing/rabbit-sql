package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.annotation.Arg;
import com.github.chengyuxing.sql.annotation.XQLMapper;
import com.github.chengyuxing.sql.transaction.Definition;
import com.github.chengyuxing.sql.transaction.Level;
import com.github.chengyuxing.sql.transaction.Tx;
import com.github.chengyuxing.sql.types.BatchResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class JdbcRegressionTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private BakiDao baki;

    @Before
    public void setUp() throws Exception {
        SQLiteDataSource source = new SQLiteDataSource();
        source.setUrl("jdbc:sqlite:" + folder.newFile("test.db").getAbsolutePath());
        baki = new BakiDao(source);
        baki.execute("create table entries (id integer primary key, name text)", Collections.emptyMap());
    }

    @Test
    public void inheritedMapperMethodsUseChildInterfaceAlias() throws Exception {
        File sql = folder.newFile("mapper.xql");
        Files.write(sql.toPath(), "/*[findValue]*/\nselect :id;\n".getBytes(StandardCharsets.UTF_8));
        XQLFileManager manager = new XQLFileManager();
        manager.add("child", sql.toURI().toString());
        baki.setXqlFileManager(manager);
        ChildMapper mapper = baki.proxyXQLMapper(ChildMapper.class);
        assertEquals(7, mapper.findValue(7));
    }

    @Test
    public void annotatedParentMethodsKeepTheirOwnAlias() throws Exception {
        File sql = folder.newFile("parent.xql");
        Files.write(sql.toPath(), "/*[findValue]*/\nselect :id + 1;\n".getBytes(StandardCharsets.UTF_8));
        XQLFileManager manager = new XQLFileManager();
        manager.add("parent", sql.toURI().toString());
        baki.setXqlFileManager(manager);
        assertEquals(8, baki.proxyXQLMapper(ChildOfAnnotatedParent.class).findValue(7));
    }

    @Test
    public void invalidBatchSizesAreRejectedEvenForEmptyBatches() {
        for (int size : new int[]{0, -1}) {
            assertThrows(IllegalArgumentException.class, () -> baki.setBatchSize(size));
            assertEquals(1000, baki.getBatchSize());
            assertThrows(IllegalArgumentException.class, () -> baki.executeBatch(Collections.emptyList(), size));
            assertThrows(IllegalArgumentException.class, () -> baki.executeBatchUpdate(
                    "insert into entries (id) values (:id)", Collections.<Map<String, ?>>emptyList(), Function.identity(), size));
        }
    }

    @Test
    public void invalidPagesAreRejectedBeforeCountQueryOrEmptyResultShortcut() {
        assertThrows(IllegalArgumentException.class, () -> baki.query("invalid SQL").pageable(0, 10));
        assertThrows(IllegalArgumentException.class, () -> baki.query("invalid SQL").pageable(1, 0));
        assertThrows(IllegalArgumentException.class, () -> baki.query("invalid SQL").pageable(100000, 100000));
        assertThrows(IllegalArgumentException.class, () -> baki.query("invalid SQL").pageable(1, 10).count(-1));
        assertTrue(baki.query("invalid SQL").pageable(1, 10).count(0).collect().getData().isEmpty());
    }

    @Test
    public void preparedBatchesBindRepeatedParametersAndFlushFinalPartialBatch() {
        BatchResult result = baki.executeBatchUpdate("insert into entries (id, name) values (:id, :id)",
                Arrays.asList(Collections.singletonMap("id", 1), Collections.singletonMap("id", 2), Collections.singletonMap("id", 3)),
                Function.identity(), 2);
        assertArrayEquals(new int[]{1, 1, 1}, result.getAffectedRows());
        assertEquals(3, baki.query("select * from entries").rows().size());
        assertEquals("3", baki.query("select name from entries where id = 3").findFirstRow().getString(0));
    }

    @Test
    public void statementsCommitAndBusinessFailuresRollBackWithRealDriver() {
        Definition definition = Definition.defaultDefinition();
        definition.setLevel(Level.SERIALIZABLE);
        Tx.using((Runnable) () -> baki.execute("insert into entries (id, name) values (1, 'committed')", Collections.emptyMap()), definition);
        RuntimeException failure = new RuntimeException("business failed");
        assertSame(failure, assertThrows(RuntimeException.class, () -> Tx.using((Runnable) () -> {
            baki.execute("insert into entries (id, name) values (2, 'rolled back')", Collections.emptyMap());
            throw failure;
        }, definition)));
        try (Stream<DataRow> rows = baki.query("select id from entries order by id").stream()) {
            assertArrayEquals(new Integer[]{1}, rows.map(row -> row.getInt(0)).toArray(Integer[]::new));
        }
    }

    public interface ParentMapper {
        int findValue(@Arg("id") int id);
    }

    @XQLMapper("child")
    public interface ChildMapper extends ParentMapper {
    }

    @XQLMapper("parent")
    public interface AnnotatedParentMapper {
        int findValue(@Arg("id") int id);
    }

    @XQLMapper("unused-child")
    public interface ChildOfAnnotatedParent extends AnnotatedParentMapper {
    }
}
