package tests;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.types.BatchResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class BatchRegressionTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();
    private BakiDao baki;

    @Before
    public void setUp() throws Exception {
        SQLiteDataSource source = new SQLiteDataSource();
        source.setUrl("jdbc:sqlite:" + folder.newFile("batch.db").getAbsolutePath());
        baki = new BakiDao(source);
        baki.execute("create table entries (id integer primary key, name text, payload blob)", Collections.emptyMap());
    }

    @Test
    public void preparedBatchConsumesOnceAndMapsEveryRowOnce() {
        AtomicInteger calls = new AtomicInteger();
        BatchResult result = baki.executeBatchUpdate("insert into entries (id, name) values (:id, :name)",
                once(Arrays.asList(1, 2, 3)), id -> {
                    calls.incrementAndGet();
                    return Args.<Object>of("id", id).add("name", "row" + id);
                }, 2);
        assertEquals(3, calls.get());
        assertArrayEquals(new int[]{1, 1, 1}, result.getAffectedRows());
        assertEquals(3, baki.query("select count(*) from entries").findFirstRow().getInt(0).intValue());
        assertEquals("row1", baki.query("select name from entries where id = 1").findFirstRow().getString(0));
    }

    @Test
    public void rawBatchConsumesOnceIncludingWatcherAndErrorReporting() {
        assertEquals(2, baki.executeBatch(once(Arrays.asList(
                "insert into entries(id) values(1)", "insert into entries(id) values(2)")), 10).getAffectedRowsCount());
        assertEquals(2, baki.query("select count(*) from entries").findFirstRow().getInt(0).intValue());
        assertThrows(RuntimeException.class, () -> baki.executeBatch(once(Collections.singletonList("invalid SQL")), 10));
    }

    @Test
    public void tableBatchesAcceptEmptyInputs() {
        assertEquals(0, baki.table("entries").enableBatch().insert(Collections.<Map<String, ?>>emptyList()).getAffectedRows().length);
        assertEquals(0, baki.table("entries").enableBatch().by("id").update(Collections.<Map<String, ?>>emptyList()).getAffectedRows().length);
    }

    @Test
    public void tableInsertAndUpdateBatchesPreserveSinglePassInputsAndMapperCounts() {
        AtomicInteger calls = new AtomicInteger();
        assertEquals(2, baki.table("entries").enableBatch().insert(once(Arrays.asList(1, 2)), id -> {
            calls.incrementAndGet();
            return Args.<Object>of("id", id).add("name", "before");
        }).getAffectedRowsCount());
        assertEquals(2, calls.get());
        calls.set(0);
        assertEquals(2, baki.table("entries").enableBatch().by("id").update(once(Arrays.asList(1, 2)), id -> {
            calls.incrementAndGet();
            return Args.<Object>of("id", id).add("name", "after");
        }).getAffectedRowsCount());
        assertEquals(2, calls.get());
        assertEquals(2, baki.query("select count(*) from entries where name = 'after'").findFirstRow().getInt(0).intValue());
    }

    @Test
    public void batchRejectsRowsThatChangeSqlShapeBeforeFlushingTheBatch() {
        List<Map<String, ?>> rows = Arrays.asList(
                Args.<Object>of("value", "1"), Args.<Object>of("value", "2 + 3"));
        assertThrows(IllegalArgumentException.class, () -> baki.executeBatchUpdate(
                "insert into entries(id) values(${value})", rows, row -> row, 10));
        assertEquals(0, baki.query("select count(*) from entries").findFirstRow().getInt(0).intValue());
    }

    @Test
    public void xqlLoopParametersAreResolvedForEveryBatchRow() throws Exception {
        Path file = folder.newFile("loop.xql").toPath();
        String sql = "/* [insert] */\ninsert into entries(id, name) values(\n" +
                "-- #for value of :values; first as isFirst\n-- #if :isFirst\n:value\n" +
                "-- #else\n,:value\n-- #fi\n-- #done\n);\n";
        Files.write(file, sql.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        XQLFileManager manager = new XQLFileManager();
        manager.add("x", file.toUri().toString());
        manager.init();
        baki.setXqlFileManager(manager);
        List<Map<String, ?>> rows = Arrays.asList(
                Args.<Object>of("values", Arrays.asList(1, "first")), Args.<Object>of("values", Arrays.asList(2, "second")));
        assertEquals(2, baki.executeBatchUpdate("&x.insert", rows, row -> row, 10).getAffectedRowsCount());
        assertEquals("first", baki.query("select name from entries where id=1").findFirstRow().getString(0));
        assertEquals("second", baki.query("select name from entries where id=2").findFirstRow().getString(0));
    }

    @Test
    public void filesAndPathsBindAsBinaryValuesWithRealDriver() throws Exception {
        Path file = folder.newFile("payload.bin").toPath();
        byte[] bytes = {0, 1, (byte) 255, 42};
        Files.write(file, bytes);
        baki.executeUpdate("insert into entries(id, payload) values(:id, :payload)", Args.<Object>of("id", 1).add("payload", file));
        baki.executeUpdate("insert into entries(id, payload) values(:id, :payload)", Args.<Object>of("id", 2).add("payload", file.toFile()));
        for (Map<String, Object> row : baki.query("select payload from entries order by id").maps()) {
            assertArrayEquals(bytes, (byte[]) row.get("payload"));
        }
        Files.delete(file);
    }

    private static <T> Iterable<T> once(List<T> items) {
        AtomicBoolean requested = new AtomicBoolean();
        Iterator<T> iterator = items.iterator();
        return () -> {
            if (!requested.compareAndSet(false, true)) {
                throw new AssertionError("Input iterator requested twice");
            }
            return iterator;
        };
    }
}
