package tests;

import baki.entityExecutor.MyEntityMetaParser;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.EntityManager;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.sql.plugins.EntityExecutor;
import com.github.chengyuxing.sql.util.JdbcUtils;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sqlite.SQLiteDataSource;

import javax.persistence.*;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class EntityConvenienceRegressionTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();
    private BakiDao baki;
    private EntityExecutor<Row> rows;
    public enum State {
        ACTIVE { @Override public String toString() { return "display label"; } },
        BLOCKED
    }

    @Before
    public void prepare() throws Exception {
        SQLiteDataSource source = new SQLiteDataSource();
        source.setUrl("jdbc:sqlite:" + folder.newFile("convenience.db").getAbsolutePath());
        baki = new BakiDao(source);
        baki.setEntityMetaProvider(new MyEntityMetaParser());
        baki.execute("create table entries (entry_id integer primary key, status_name text, ordinal_state integer, plain_state text)", Collections.emptyMap());
        rows = baki.entity(Row.class);
    }

    private Row row(long id, State state) {
        Row row = new Row();
        row.setId(id);
        row.setState(state);
        row.setOrdinalState(state);
        row.setPlainState(state);
        return row;
    }

    @Test
    public void entityEnumsRoundTripByNameAndAnnotatedOrdinal() {
        assertEquals(1, rows.insert().save(row(1, State.ACTIVE)));
        assertEquals("ACTIVE", baki.query("select status_name from entries").findFirstRow().getString(0));
        assertEquals("ACTIVE", baki.query("select plain_state from entries").findFirstRow().getString(0));
        assertEquals(Integer.valueOf(0), baki.query("select ordinal_state from entries").findFirstRow().getInt(0));
        Row read = rows.findById(1L).get();
        assertSame(State.ACTIVE, read.getState());
        assertSame(State.ACTIVE, read.getOrdinalState());
        assertSame(State.ACTIVE, read.getPlainState());
        assertEquals(1, rows.update().save(row(1, State.BLOCKED)));
        assertSame(State.BLOCKED, rows.findById(1L).get().getOrdinalState());
        assertEquals(1, rows.update().only(Row::getState, Row::getOrdinalState).withNullValues().save(row(1, null)));
        read = rows.findById(1L).get();
        assertNull(read.getState());
        assertNull(read.getOrdinalState());
        assertSame(State.BLOCKED, read.getPlainState());
    }

    @Test
    public void enumsWorkInBothBatchPathsIncludingOnlyAndExclude() {
        for (boolean withNulls : new boolean[]{false, true}) {
            baki.execute("delete from entries", Collections.emptyMap());
            com.github.chengyuxing.sql.dsl.Insert<Row> insert = rows.insert();
            if (withNulls) insert.withNullValues();
            AtomicInteger iteratorCalls = new AtomicInteger();
            Iterable<Row> once = () -> {
                assertEquals(1, iteratorCalls.incrementAndGet());
                return Arrays.asList(row(1, State.ACTIVE), row(2, State.BLOCKED)).iterator();
            };
            assertArrayEquals(new int[]{1, 1}, insert.save(once).getAffectedRows());
            com.github.chengyuxing.sql.dsl.Update<Row> update = rows.update().exclude(Row::getState);
            if (withNulls) update.withNullValues();
            assertArrayEquals(new int[]{1, 1}, update.save(Arrays.asList(row(1, State.BLOCKED), row(2, State.ACTIVE))).getAffectedRows());
            assertSame(State.ACTIVE, rows.findById(1L).get().getState());
            assertSame(State.BLOCKED, rows.findById(1L).get().getOrdinalState());
            assertArrayEquals(new int[]{1, 1}, rows.update().only(Row::getOrdinalState).withNullValues()
                    .save(Arrays.asList(row(1, null), row(2, null))).getAffectedRows());
            assertNull(rows.findById(1L).get().getOrdinalState());
            assertArrayEquals(new int[]{1, 1}, rows.delete().execute(Arrays.asList(row(1, State.ACTIVE), row(2, State.ACTIVE))).getAffectedRows());
        }
    }

    @Test
    public void setterAndAllConditionShapesUseTheFieldConversion() {
        assertEquals(1, rows.insert().set(Row::getId, 1L).set(Row::getState, State.ACTIVE)
                .set(Row::getOrdinalState, State.BLOCKED).save());
        assertTrue(rows.query().where(w -> w.eq(Row::getState, State.ACTIVE).eq(Row::getOrdinalState, State.BLOCKED)).exists());
        assertTrue(rows.query().where(w -> w.in(Row::getOrdinalState, Arrays.asList(State.ACTIVE, State.BLOCKED))).exists());
        assertFalse(rows.query().where(w -> w.notIn(Row::getOrdinalState, Collections.singletonList(State.BLOCKED))).exists());
        assertTrue(rows.query().where(w -> w.between(Row::getOrdinalState, State.ACTIVE, State.BLOCKED)).exists());
        assertTrue(rows.query().where(w -> w.and(g -> g.eq(Row::getOrdinalState, State.ACTIVE)
                .eq(Row::getOrdinalState, State.BLOCKED))).exists());
        assertTrue(rows.query().where(w -> w.or(g -> g.eq(Row::getState, State.ACTIVE)
                .eq(Row::getOrdinalState, State.BLOCKED))).exists());
        assertEquals(1, rows.update().where(w -> w.eq(Row::getOrdinalState, State.BLOCKED)).set(Row::getOrdinalState, State.ACTIVE).save());
        assertSame(State.ACTIVE, rows.findById(1L).get().getOrdinalState());
        assertEquals(1, rows.delete().where(w -> w.in(Row::getOrdinalState, Collections.singletonList(State.ACTIVE))).execute());
    }

    @Test
    public void existsDoesNotMapRowsCountOrApplyProjectionAndOrder() {
        assertFalse(rows.query().exists());
        rows.insert().save(row(1, State.ACTIVE));
        baki.execute("update entries set status_name = 'INVALID'", Collections.emptyMap());
        List<String> sqls = new ArrayList<>();
        baki.setSqlInterceptor((rawSql, sql, args, info) -> { sqls.add(sql.toLowerCase(Locale.ROOT)); return sql; });
        assertTrue(rows.existsById(1L));
        com.github.chengyuxing.sql.dsl.Query<Row, ?> query = rows.query("trace").select(Row::getState)
                .where(w -> w.eq(Row::getId, 1L)).orderBy(o -> o.desc(Row::getState));
        assertTrue(query.exists());
        assertEquals(2, sqls.size());
        for (String sql : sqls) {
            assertTrue(sql.contains("select 1 from entries"));
            assertTrue(sql.contains("limit"));
            assertFalse(sql.contains("count("));
            assertFalse(sql.contains("order by"));
            assertFalse(sql.contains("status_name"));
        }
        // The existence check must not mutate the builder's projection or ordering.
        assertThrows(IllegalStateException.class, query::list);
        assertThrows(IllegalStateException.class, () -> rows.findById(1L));
        assertEquals(1, rows.deleteById(1L));
        assertFalse(rows.existsById(1L));
        assertEquals(0, rows.deleteById(1L));
    }

    @Test
    public void primaryKeyHelpersValidateNullAndBindValuesSafely() {
        rows.insert().save(row(1, State.ACTIVE));
        assertFalse(rows.existsById("1 OR 1=1"));
        assertEquals(0, rows.deleteById("1 OR 1=1"));
        assertTrue(rows.existsById(1L));
        assertFalse(rows.existsById(99L));
        baki.setSqlInterceptor((rawSql, sql, args, info) -> { throw new AssertionError("Null key must fail before SQL"); });
        assertThrows(IllegalArgumentException.class, () -> rows.existsById(null));
        assertThrows(IllegalArgumentException.class, () -> rows.deleteById(null));
        assertThrows(IllegalArgumentException.class, () -> rows.findById(null));
    }

    @Test
    public void ordinalPrimaryKeysUseTheSameConversionAcrossAllOperations() {
        baki.execute("create table enum_keys (key_code integer primary key, value text)", Collections.emptyMap());
        EntityExecutor<EnumKey> keys = baki.entity(EnumKey.class);
        EnumKey key = new EnumKey();
        key.setId(State.BLOCKED);
        key.setValue("first");
        assertEquals(1, keys.insert().save(key));
        assertTrue(keys.existsById(State.BLOCKED));
        assertFalse(keys.existsById(State.ACTIVE));
        assertSame(State.BLOCKED, keys.findById(State.BLOCKED).get().getId());
        key.setValue("second");
        assertEquals(1, keys.update().save(key));
        assertEquals("second", keys.findById(State.BLOCKED).get().getValue());
        assertEquals(1, keys.deleteById(State.BLOCKED));
        assertEquals(1, keys.insert().save(key));
        assertEquals(1, keys.delete().execute(key));
    }

    @Test
    public void rawSqlEnumsBindNamesInsteadOfDisplayLabels() throws Exception {
        PreparedStatement ps = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    assertEquals("setString", method.getName());
                    assertEquals(1, args[0]);
                    assertEquals("ACTIVE", args[1]);
                    return null;
                });
        JdbcUtils.setStatementValue(ps, 1, State.ACTIVE);
        rows.insert().save(row(1, State.ACTIVE));
        assertEquals(1, baki.query("select entry_id from entries where status_name = :state")
                .args("state", State.ACTIVE).rows().size());
        assertSame(State.ACTIVE, baki.query("select * from entries").findFirstEntity(Row.class).getState());
    }

    @Test
    public void existenceUsesDialectPagingAndClosesBothEmptyAndNonemptyStreams() {
        for (PageHelper pager : new PageHelper[]{new PGPageHelper(), new MysqlPageHelper(),
                new OraclePageHelper(), new Db2PageHelper(), new SqlServer2012PageHelper()}) {
            AtomicBoolean closed = new AtomicBoolean();
            AtomicBoolean present = new AtomicBoolean(true);
            AtomicInteger executions = new AtomicInteger();
            SQLiteDataSource source = new SQLiteDataSource();
            source.setUrl("jdbc:sqlite::memory:");
            BakiDao probe = new BakiDao(source) {
                @Override protected PageHelper builtinPager() { return pager; }
                @Override public Stream<DataRow> executeQueryStream(String sql, Map<String, ?> args) {
                    executions.incrementAndGet();
                    assertTrue(sql.contains("select 1 from entries"));
                    assertFalse(sql.contains("count("));
                    assertEquals(1L, args.get("entry_id"));
                    if (pager instanceof SqlServer2012PageHelper) {
                        assertTrue(sql.contains("order by entry_id"));
                        assertTrue(sql.contains("fetch next"));
                    }
                    Stream<DataRow> result = present.get() ? Stream.of(DataRow.of("1", 1)) : Stream.empty();
                    return result.onClose(() -> closed.set(true));
                }
            };
            probe.setEntityMetaProvider(new MyEntityMetaParser());
            assertTrue(probe.entity(Row.class).existsById(1L));
            assertTrue(closed.get());
            closed.set(false);
            present.set(false);
            assertFalse(probe.entity(Row.class).existsById(1L));
            assertTrue(closed.get());
            assertEquals(2, executions.get());
        }
    }

    @Test
    public void enumStrategyCanBeConfiguredEntirelyByColumnMetadataWithoutJpa() {
        baki.setEntityMetaProvider(new EntityManager.EntityMetaProvider() {
            @Override public String tableName(Class<?> type) { return "entries"; }
            @Override public EntityManager.ColumnMeta columnMeta(java.lang.reflect.Field field) {
                boolean id = field.getName().equals("id");
                EntityManager.ColumnMeta meta = new EntityManager.ColumnMeta(id ? "entry_id" : "ordinal_state");
                meta.setPrimaryKey(id);
                if (!id) meta.setEnumerated(EntityManager.ColumnEnumType.ORDINAL);
                return meta;
            }
            @Override public Object columnValue(java.lang.reflect.Field field, Object value) {
                return ValueUtils.adaptValue(field.getType(), value);
            }
        });
        EntityExecutor<MetadataRow> executor = baki.entity(MetadataRow.class);
        MetadataRow row = new MetadataRow();
        row.setId(1L);
        row.setState(State.BLOCKED);
        assertEquals(1, executor.insert().save(row));
        assertEquals(Integer.valueOf(1), baki.query("select ordinal_state from entries").findFirstRow().getInt(0));
        assertSame(State.BLOCKED, executor.findById(1L).get().getState());
        assertTrue(executor.query().where(w -> w.eq(MetadataRow::getState, State.BLOCKED)).exists());
        assertEquals(1, executor.update().where(w -> w.eq(MetadataRow::getId, 1L))
                .set(MetadataRow::getState, State.ACTIVE).save());
        assertSame(State.ACTIVE, executor.findById(1L).get().getState());
        assertEquals(1, executor.deleteById(1L));
    }

    @Test
    public void enumStrategyParticipatesInColumnMetadataEqualityAndHashing() {
        EntityManager.ColumnMeta left = new EntityManager.ColumnMeta("state");
        EntityManager.ColumnMeta right = new EntityManager.ColumnMeta("state");
        assertNull(left.getEnumerated());
        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        right.setEnumerated(EntityManager.ColumnEnumType.ORDINAL);
        assertNotEquals(left, right);
        left.setEnumerated(EntityManager.ColumnEnumType.STRING);
        assertNotEquals(left, right);
        left.setEnumerated(EntityManager.ColumnEnumType.ORDINAL);
        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertEquals(1, new HashSet<>(Arrays.asList(left, right)).size());
        left.setEnumerated(null);
        right.setEnumerated(null);
        assertEquals(left, right);
    }

    public static class MetadataRow {
        private Long id;
        private State state;
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public State getState() { return state; }
        public void setState(State state) { this.state = state; }
    }

    public static class BaseRow {
        @Id @Column(name = "entry_id") private Long id;
        @Enumerated(EnumType.STRING) @Column(name = "status_name") private State state;
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public State getState() { return state; }
        public void setState(State state) { this.state = state; }
    }

    @Entity @Table(name = "entries")
    public static class Row extends BaseRow {
        @Enumerated @Column(name = "ordinal_state") private State ordinalState;
        @Column(name = "plain_state") private State plainState;
        public State getOrdinalState() { return ordinalState; }
        public void setOrdinalState(State state) { ordinalState = state; }
        public State getPlainState() { return plainState; }
        public void setPlainState(State state) { plainState = state; }
    }

    @Entity @Table(name = "enum_keys")
    public static class EnumKey {
        @Id @Enumerated @Column(name = "key_code") private State id;
        private String value;
        public State getId() { return id; }
        public void setId(State id) { this.id = id; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }
}
