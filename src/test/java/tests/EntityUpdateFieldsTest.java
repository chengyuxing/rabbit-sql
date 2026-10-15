package tests;

import baki.entityExecutor.MyEntityMetaParser;
import com.github.chengyuxing.common.MethodReference;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.dsl.Update;
import com.github.chengyuxing.sql.plugins.EntityExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sqlite.SQLiteDataSource;

import javax.persistence.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class EntityUpdateFieldsTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private BakiDao baki;
    private EntityExecutor<User> users;

    @Before
    public void prepare() throws Exception {
        SQLiteDataSource source = new SQLiteDataSource();
        source.setUrl("jdbc:sqlite:" + folder.newFile("exclude.db").getAbsolutePath());
        baki = new BakiDao(source);
        baki.setEntityMetaProvider(new MyEntityMetaParser() {
            @Override
            public Object columnValue(Field field, Object value) {
                return ValueUtils.adaptValue(field.getType(), value);
            }
        });
        baki.execute("create table users (id integer primary key, display_name text, password_hash text, created_by text)", Collections.emptyMap());
        users = baki.entity(User.class);
        users.insert().save(Arrays.asList(user(1L, "first", "secret-1"), user(2L, "second", "secret-2")));
    }

    private User user(Long id, String name, String password) {
        User user = new User();
        user.setId(id);
        user.setName(name);
        user.setPassword(password);
        user.setCreatedBy("original");
        return user;
    }

    @Test
    public void singleSaveExcludesRenamedPropertyAndKeepsMetadataRestrictions() {
        User value = user(1L, "updated", "replacement");
        value.setCreatedBy("changed");
        assertEquals(1, users.update().exclude(User::getPassword).save(value));
        User read = users.findById(1L).get();
        assertEquals("updated", read.getName());
        assertEquals("secret-1", read.getPassword());
        assertEquals("original", read.getCreatedBy());
        assertEquals("replacement", value.getPassword());
    }

    @Test
    public void inheritedPropertiesAndRepeatedExclusionsAreSupported() {
        Update<User> update = users.update().exclude(User::getName).exclude(User::getName);
        assertEquals(1, update.save(user(1L, "ignored", "changed")));
        assertEquals("first", users.findById(1L).get().getName());
        assertEquals("changed", users.findById(1L).get().getPassword());
        update.exclude(User::getPassword);
        assertEquals(0, update.save(user(2L, "ignored", "ignored")));
    }

    @Test
    public void configurationDoesNotLeakIntoOtherBuildersOrSharedMetadata() {
        Update<User> excluded = users.update().exclude(User::getPassword);
        Update<User> normal = users.update();
        assertEquals(1, excluded.save(user(1L, "changed", "ignored")));
        assertEquals(1, normal.save(user(1L, "changed", "new-secret")));
        assertEquals("new-secret", users.findById(1L).get().getPassword());
        assertEquals(1, baki.entity(User.class).update().withNullValues().save(user(2L, "changed", "other-secret")));
        assertEquals("other-secret", users.findById(2L).get().getPassword());
    }

    @Test
    public void nullHandlingAndConfigurationOrderRemainUnchanged() {
        User value = user(1L, null, null);
        assertEquals(0, users.update().exclude(User::getPassword).save(value));
        assertEquals("first", users.findById(1L).get().getName());
        assertEquals(1, users.update().withNullValues().exclude(User::getPassword).save(value));
        assertNull(users.findById(1L).get().getName());
        assertEquals("secret-1", users.findById(1L).get().getPassword());
        assertEquals(1, users.update().exclude(User::getPassword).withNullValues().save(user(2L, null, null)));
        assertNull(users.findById(2L).get().getName());
        assertEquals("secret-2", users.findById(2L).get().getPassword());
    }

    @Test
    public void batchesApplyExclusionsForBothNullModesAndConsumeIterableOnce() {
        for (boolean withNulls : new boolean[]{false, true}) {
            Update<User> update = users.update().exclude(User::getPassword);
            if (withNulls) update.withNullValues();
            List<User> values = Arrays.asList(user(1L, "changed", "ignored"), user(2L, null, "ignored"));
            AtomicInteger calls = new AtomicInteger();
            Iterable<User> once = () -> {
                assertEquals(1, calls.incrementAndGet());
                return values.iterator();
            };
            assertArrayEquals(withNulls ? new int[]{1, 1} : new int[]{1, 0}, update.save(once).getAffectedRows());
            assertEquals("changed", users.findById(1L).get().getName());
            assertEquals(withNulls ? null : "second", users.findById(2L).get().getName());
            assertEquals("secret-1", users.findById(1L).get().getPassword());
            assertEquals("secret-2", users.findById(2L).get().getPassword());
        }
    }

    @Test
    public void excludingEveryColumnProducesZeroResultsWithoutExecutingSql() {
        baki.setSqlInterceptor((rawSql, sql, args, meta) -> { throw new AssertionError("No SQL expected: " + sql); });
        for (boolean withNulls : new boolean[]{false, true}) {
            Update<User> update = users.update().exclude(User::getName, User::getPassword);
            if (withNulls) update.withNullValues();
            assertEquals(0, update.save(user(1L, "ignored", "ignored")));
            assertArrayEquals(new int[]{0, 0}, update.save(Arrays.asList(user(1L, null, null), user(2L, null, null))).getAffectedRows());
            assertArrayEquals(new int[0], update.save(Collections.emptyList()).getAffectedRows());
            assertThrows(IllegalArgumentException.class, () -> update.save(user(null, null, null)));
            assertThrows(IllegalArgumentException.class, () -> update.save(Collections.singletonList(user(null, null, null))));
        }
    }

    @Test
    public void excludingPrimaryKeyDoesNotRemoveItsLookupOrValidation() {
        for (boolean withNulls : new boolean[]{false, true}) {
            Update<User> update = users.update().exclude(User::getId, User::getCreatedBy, User::getPassword);
            if (withNulls) update.withNullValues();
            assertEquals(1, update.save(user(1L, "target", "ignored")));
            assertEquals("second", users.findById(2L).get().getName());
            assertThrows(IllegalArgumentException.class, () -> update.save(user(null, "bad", null)));
            assertThrows(IllegalArgumentException.class, () -> update.save(Collections.singletonList(user(null, "bad", null))));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void invalidReferencesFailWithoutPartiallyChangingConfiguration() {
        Update<User> update = users.update();
        assertThrows(IllegalArgumentException.class, () -> update.exclude(User::getPassword, User::getComputed));
        assertThrows(IllegalArgumentException.class, () -> update.exclude(User::getPassword, User::getScratch));
        assertThrows(NullPointerException.class, () -> update.exclude((MethodReference<User>[]) null));
        assertThrows(NullPointerException.class, () -> update.exclude(User::getPassword, null));
        assertEquals(1, update.exclude().save(user(1L, "updated", "changed")));
        assertEquals("changed", users.findById(1L).get().getPassword());
    }

    @Test
    public void conditionalUpdatesCannotSilentlyIgnoreExclusionsIncludingBuilderReuse() {
        Update<User> update = users.update();
        Update.UpdateSetter<User> setter = update.where(w -> w.eq(User::getId, 1L)).set(User::getName, "ignored");
        update.exclude(User::getPassword);
        assertThrows(IllegalStateException.class, setter::save);
        assertThrows(IllegalStateException.class, () -> update.where(w -> w.eq(User::getId, 1L)).set(User::getName, "ignored").save());
        assertEquals("first", users.findById(1L).get().getName());
        assertEquals(1, users.update().where(w -> w.eq(User::getId, 1L)).set(User::getName, "explicit").save());
    }

    @Test
    public void onlySelectsInheritedRenamedFieldsAndPreservesSingleSaveNullSemantics() {
        Update<User> update = users.update().only(User::getName);
        assertEquals(1, update.save(user(1L, "changed", "ignored")));
        assertEquals("changed", users.findById(1L).get().getName());
        assertEquals("secret-1", users.findById(1L).get().getPassword());
        assertEquals(0, update.save(user(1L, null, "ignored")));
        assertEquals("changed", users.findById(1L).get().getName());
        assertEquals(1, update.withNullValues().save(user(1L, null, "ignored")));
        assertNull(users.findById(1L).get().getName());
        assertEquals("secret-1", users.findById(1L).get().getPassword());
    }

    @Test
    public void onlyAccumulatesPropertiesWithoutLeakingIntoOtherBuilders() {
        Update<User> update = users.update().only(User::getName).only(User::getName, User::getPassword);
        assertEquals(1, update.save(user(1L, "both", "changed")));
        assertEquals("both", users.findById(1L).get().getName());
        assertEquals("changed", users.findById(1L).get().getPassword());
        Update<User> narrow = users.update().only(User::getPassword);
        assertEquals(1, narrow.save(user(2L, "ignored", "new-secret")));
        assertEquals("second", users.findById(2L).get().getName());
        assertEquals(1, users.update().save(user(2L, "default", "normal")));
        assertEquals("default", users.findById(2L).get().getName());
        assertEquals("normal", users.findById(2L).get().getPassword());
    }

    @Test
    public void onlyWorksInBothBatchModesWithSinglePassIterables() {
        for (boolean withNulls : new boolean[]{false, true}) {
            Update<User> update = users.update();
            if (withNulls) update.withNullValues();
            update.only(User::getName);
            AtomicInteger calls = new AtomicInteger();
            Iterable<User> once = () -> {
                assertEquals(1, calls.incrementAndGet());
                return Arrays.asList(user(1L, "changed", "ignored"), user(2L, null, "ignored")).iterator();
            };
            assertArrayEquals(withNulls ? new int[]{1, 1} : new int[]{1, 0}, update.save(once).getAffectedRows());
            assertEquals("changed", users.findById(1L).get().getName());
            assertEquals(withNulls ? null : "second", users.findById(2L).get().getName());
            assertEquals("secret-1", users.findById(1L).get().getPassword());
            assertEquals("secret-2", users.findById(2L).get().getPassword());
            assertArrayEquals(new int[0], update.save(Collections.emptyList()).getAffectedRows());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void invalidOnlyCallsDoNotChangeSelectionOrLockTheMode() {
        Update<User> update = users.update();
        assertThrows(IllegalArgumentException.class, () -> update.only());
        assertThrows(IllegalArgumentException.class, () -> update.only(User::getName, User::getId));
        assertThrows(IllegalArgumentException.class, () -> update.only(User::getName, User::getCreatedBy));
        assertThrows(IllegalArgumentException.class, () -> update.only(User::getName, User::getScratch));
        assertThrows(IllegalArgumentException.class, () -> update.only(User::getName, User::getComputed));
        assertThrows(NullPointerException.class, () -> update.only((MethodReference<User>[]) null));
        assertThrows(NullPointerException.class, () -> update.only(User::getName, null));
        assertEquals(1, update.exclude(User::getName).save(user(1L, "ignored", "changed")));
        assertEquals("first", users.findById(1L).get().getName());
        assertEquals("changed", users.findById(1L).get().getPassword());
        Update<User> selected = users.update().only(User::getName);
        assertThrows(IllegalArgumentException.class, () -> selected.only(User::getPassword, User::getId));
        assertThrows(IllegalArgumentException.class, () -> selected.only());
        assertEquals(1, selected.save(user(2L, "changed", "ignored")));
        assertEquals("secret-2", users.findById(2L).get().getPassword());
    }

    @Test
    public void onlyAndExcludeRejectMixingImmediatelyInEitherOrder() {
        Update<User> only = users.update().only(User::getName);
        assertThrows(IllegalStateException.class, () -> only.exclude(User::getPassword));
        assertThrows(IllegalStateException.class, () -> only.exclude());
        assertEquals(1, only.save(user(1L, "selected", "ignored")));
        assertEquals("secret-1", users.findById(1L).get().getPassword());
        Update<User> exclude = users.update().exclude(User::getName);
        assertThrows(IllegalStateException.class, () -> exclude.only(User::getName));
        assertEquals(1, exclude.save(user(2L, "ignored", "changed")));
        assertEquals("second", users.findById(2L).get().getName());
        assertEquals("changed", users.findById(2L).get().getPassword());
        // An empty exclusion is a no-op, so it does not establish a mode.
        assertEquals(1, users.update().exclude().only(User::getName).save(user(2L, "selected", "ignored")));
    }

    @Test
    public void onlyStillRequiresPrimaryKeysForEverySaveEntryPoint() {
        for (boolean withNulls : new boolean[]{false, true}) {
            Update<User> update = users.update().only(User::getName);
            if (withNulls) update.withNullValues();
            assertThrows(IllegalArgumentException.class, () -> update.save(user(null, null, null)));
            assertThrows(IllegalArgumentException.class, () -> update.save(Collections.singletonList(user(null, null, null))));
        }
    }

    @Test
    public void onlyDoesNotChangeConditionalSetterBehavior() {
        Update<User> update = users.update();
        Update.UpdateSetter<User> setter = update.where(w -> w.eq(User::getId, 1L)).set(User::getName, "ignored");
        update.only(User::getName);
        assertThrows(IllegalStateException.class, setter::save);
        assertThrows(IllegalStateException.class, () -> update.where(w -> w.eq(User::getId, 1L)).set(User::getName, "ignored").save());
        assertEquals("first", users.findById(1L).get().getName());
        assertEquals(1, users.update().where(w -> w.eq(User::getId, 1L)).set(User::getName, null).save());
        assertNull(users.findById(1L).get().getName());
        assertThrows(IllegalArgumentException.class, () -> users.update().where(w -> w.eq(User::getId, 2L)).set(User::getId, 3L));
        assertThrows(IllegalArgumentException.class, () -> users.update().where(w -> w.eq(User::getId, 2L)).set(User::getCreatedBy, "changed"));
    }

    public static class BaseUser {
        @Id private Long id;
        @Column(name = "display_name") private String name;
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    @Entity
    @Table(name = "users")
    public static class User extends BaseUser {
        @Column(name = "password_hash") private String password;
        @Column(name = "created_by", updatable = false) private String createdBy;
        @Transient private String scratch;
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getCreatedBy() { return createdBy; }
        public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
        public String getScratch() { return scratch; }
        public void setScratch(String scratch) { this.scratch = scratch; }
        public String getComputed() { return "not a mapped field"; }
    }
}
