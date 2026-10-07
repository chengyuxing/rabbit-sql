package tests;

import baki.entityExecutor.MyEntityMetaParser;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.EntityManager;
import com.github.chengyuxing.sql.plugins.EntityExecutor;
import com.github.chengyuxing.sql.util.SqlGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sqlite.SQLiteDataSource;

import javax.persistence.*;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.Assert.*;

public class EntityRegressionTest {
    @Test
    public void insertSettersKeepTheLatestValueIncludingAfterReuse() {
        com.github.chengyuxing.sql.dsl.Insert.InsertSetter<Row> setter = baki.entity(Row.class).insert()
                .set(Row::getName, "first").set(Row::getName, "last").set(Row::getId, 1L);
        assertEquals(1, setter.save());
        assertEquals("last", baki.query("select name from entries where entry_id=1").findFirstRow().getString(0));
        assertEquals(1, setter.set(Row::getId, 2L).set(Row::getName, "reused").save());
        assertEquals("reused", baki.query("select name from entries where entry_id=2").findFirstRow().getString(0));
    }

    @Test
    public void everyUpdateEntryPointRejectsNullPrimaryKeys() {
        EntityExecutor<Row> executor = baki.entity(Row.class);
        Row row = new Row();
        row.setName("missing id");
        assertThrows(IllegalArgumentException.class, () -> executor.update().save(row));
        assertThrows(IllegalArgumentException.class, () -> executor.update().withNullValues().save(row));
        assertThrows(IllegalArgumentException.class, () -> executor.update().save(Collections.singletonList(row)));
        assertThrows(IllegalArgumentException.class, () -> executor.update().withNullValues().save(Collections.singletonList(row)));
    }

    @Test
    public void emptyNestedGroupsAreSkippedAndCannotAuthorizeUnconditionalWrites() {
        EntityExecutor<Row> executor = baki.entity(Row.class);
        Row row = new Row();
        row.setId(1L);
        executor.insert().save(row);
        assertEquals(1L, executor.query().where(w -> w.eq(Row::getId, 1L)
                .or(g -> g.eq(Row::getId, 2L, false))).count());
        assertEquals(1L, executor.query().where(w -> w.and(g -> g.eq(Row::getId, 2L, false))
                .eq(Row::getId, 1L)).count());
        assertThrows(IllegalArgumentException.class, () -> executor.delete()
                .where(w -> w.or(g -> g.and(inner -> inner.eq(Row::getId, 2L, false)))).execute());
        assertThrows(IllegalArgumentException.class, () -> executor.update()
                .where(w -> w.and(g -> g.eq(Row::getId, 2L, false))).set(Row::getName, "unsafe").save());
        assertEquals(1L, executor.query().count());
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private BakiDao baki;
    private final MyEntityMetaParser provider = new MyEntityMetaParser() {
        @Override
        public Object columnValue(Field field, Object value) {
            return ValueUtils.adaptValue(field.getType(), value);
        }
    };

    @Before
    public void setUp() throws Exception {
        SQLiteDataSource source = new SQLiteDataSource();
        source.setUrl("jdbc:sqlite:" + folder.newFile("entity.db").getAbsolutePath());
        baki = new BakiDao(source);
        baki.setEntityMetaProvider(provider);
        baki.execute("create table entries (entry_id integer primary key, name text, active integer)", Collections.emptyMap());
        baki.execute("create table only_ids (entry_id integer primary key)", Collections.emptyMap());
        baki.execute("create table identity_ids (id integer primary key autoincrement)", Collections.emptyMap());
    }

    @Test
    public void overriddenGettersKeepInheritedIdAndColumnAnnotationsDuringCrud() {
        EntityExecutor<OverrideRow> executor = baki.entity(OverrideRow.class);
        OverrideRow row = new OverrideRow();
        row.setId(7L);
        row.setName("before");
        row.setActive(true);
        assertEquals(1, executor.insert().save(row));
        OverrideRow read = executor.query(7L).select(OverrideRow::getId, OverrideRow::getName).findFirst().get();
        assertEquals(Long.valueOf(7), read.getId());
        assertEquals("before", read.getName());
        assertEquals(1L, executor.query().where(w -> w.eq(OverrideRow::isActive, true)).count());
        row.setName("after");
        assertEquals(1, executor.update().save(row));
        assertEquals("after", executor.query(7L).select(OverrideRow::getName).findFirst().get().getName());
        assertEquals(1, executor.delete().execute(row));
        assertEquals(0L, executor.query().count());
    }

    @Test
    public void multiplePrimaryKeysAreRejectedBeforeCrudCanUseAPartialKey() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> baki.entity(TwoKeys.class));
        assertTrue(error.getMessage().contains("Multiple primary keys"));
        assertTrue(error.getMessage().contains("entry_id"));
        assertTrue(error.getMessage().contains("tenant"));
    }

    @Test
    public void duplicateColumnMappingsAreRejectedWithEntityAndColumnDetails() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> baki.entity(Duplicate.class));
        assertTrue(error.getMessage().contains("Duplicate column 'name'"));
        assertTrue(error.getMessage().contains(Duplicate.class.getName()));
    }

    @Test
    public void providerReplacementRebuildsCachedSql() {
        EntityManager manager = new EntityManager(new SqlGenerator(':'));
        manager.setEntityMetaProvider(provider);
        EntityManager.EntityMeta old = manager.getEntityMeta(Row.class);
        manager.setEntityMetaProvider(new MyEntityMetaParser() {
            @Override public String tableName(Class<?> clazz) { return "other_entries"; }
        });
        EntityManager.EntityMeta fresh = manager.getEntityMeta(Row.class);
        assertNotSame(old, fresh);
        assertEquals("other_entries", fresh.getTableName());
        assertTrue(fresh.getInsert().contains("other_entries"));
        assertTrue(fresh.getUpdateById().contains("other_entries"));
        assertSame(fresh, manager.getEntityMeta(Row.class));
    }

    @Test
    public void blankTableAndColumnNamesAreRejectedWithEntityDetails() {
        EntityManager manager = new EntityManager(new SqlGenerator(':'));
        manager.setEntityMetaProvider(new MyEntityMetaParser() {
            @Override public String tableName(Class<?> clazz) { return " "; }
        });
        assertTrue(assertThrows(IllegalArgumentException.class, () -> manager.getEntityMeta(Row.class))
                .getMessage().contains(Row.class.getName()));
        manager.setEntityMetaProvider(new MyEntityMetaParser() {
            @Override public EntityManager.ColumnMeta columnMeta(Field field) {
                EntityManager.ColumnMeta meta = super.columnMeta(field);
                if (field.getName().equals("name")) meta.setName("");
                return meta;
            }
        });
        assertTrue(assertThrows(IllegalArgumentException.class, () -> manager.getEntityMeta(Row.class))
                .getMessage().contains(Row.class.getName() + ".name"));
    }

    @Test
    public void manualIdIsRequiredForEveryInsertEntryPoint() {
        EntityExecutor<Row> executor = baki.entity(Row.class);
        Row row = new Row();
        row.setName("missing id");
        assertThrows(IllegalArgumentException.class, () -> executor.insert().save(row));
        assertThrows(IllegalArgumentException.class, () -> executor.insert().withNullValues().save(row));
        assertThrows(IllegalArgumentException.class, () -> executor.insert().save(Collections.singletonList(row)));
        assertThrows(IllegalArgumentException.class, () -> executor.insert().withNullValues().save(Collections.singletonList(row)));
        assertThrows(IllegalArgumentException.class, () -> executor.insert().set(Row::getName, "missing id").save());
        assertThrows(IllegalArgumentException.class, () -> executor.insert().set(Row::getId, null).save());
        assertEquals(0L, executor.query().count());
        assertEquals(1, executor.insert().set(Row::getName, "valid").set(Row::getId, 9L).save());
        assertEquals(1L, executor.query(9L).count());
    }

    @Test
    public void primaryKeyOnlyEntitiesSupportInsertQueryAndDelete() {
        EntityExecutor<IdOnly> executor = baki.entity(IdOnly.class);
        IdOnly row = new IdOnly();
        row.setId(3L);
        assertEquals(1, executor.insert().save(row));
        assertEquals(Long.valueOf(3), executor.query(3L).findFirst().get().getId());
        assertTrue(assertThrows(IllegalStateException.class, executor::update)
                .getMessage().contains("No updatable columns"));
        assertEquals(1, executor.delete().execute(row));
    }

    @Test
    public void identityOnlyEntitiesInsertDefaultValuesForSingleAndBatchInserts() {
        EntityExecutor<IdentityOnly> executor = baki.entity(IdentityOnly.class);
        assertEquals(1, executor.insert().save(new IdentityOnly()));
        assertEquals(1, executor.insert().withNullValues().save(new IdentityOnly()));
        executor.insert().withNullValues().save(Collections.singletonList(new IdentityOnly()));
        assertEquals(3L, executor.query().count());
        assertNotNull(executor.query().findFirst().get().getId());
    }

    @MappedSuperclass
    public static class BaseId {
        @Id @Column(name = "entry_id")
        private Long id;
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
    }

    @Entity @Table(name = "entries")
    public static class Row extends BaseId {
        private String name;
        private boolean active;
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public boolean isActive() { return active; }
        public void setActive(boolean active) { this.active = active; }
    }

    @Entity @Table(name = "entries")
    public static class OverrideRow extends Row {
        @Override public Long getId() { return super.getId(); }
    }

    @Entity @Table(name = "entries")
    public static class TwoKeys extends Row {
        @Id private Long tenant;
        public Long getTenant() { return tenant; }
        public void setTenant(Long tenant) { this.tenant = tenant; }
    }

    @Entity @Table(name = "entries")
    public static class Duplicate extends Row {
        @Column(name = "name") private String alias;
        public String getAlias() { return alias; }
        public void setAlias(String alias) { this.alias = alias; }
    }

    @Entity @Table(name = "only_ids")
    public static class IdOnly extends BaseId {
    }

    @Entity @Table(name = "identity_ids")
    public static class IdentityOnly {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
    }
}
