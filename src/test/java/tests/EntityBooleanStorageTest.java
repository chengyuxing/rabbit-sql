package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.EntityManager;
import com.github.chengyuxing.sql.EntityManager.BooleanStorage;
import com.github.chengyuxing.sql.plugins.EntityExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sqlite.SQLiteDataSource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class EntityBooleanStorageTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private SQLiteDataSource source;
    private BakiDao baki;
    private EntityExecutor<Row> rows;

    @Before
    public void setUp() throws Exception {
        source = new SQLiteDataSource();
        source.setUrl("jdbc:sqlite:" + folder.newFile("booleans.db").getAbsolutePath());
        baki = new BakiDao(source);
        baki.setEntityMetaProvider(new Provider());
        baki.execute("create table boolean_entries (id integer primary key, numeric_flag integer, "
                + "text_flag varchar(1), yes_no_flag varchar(1), primitive_flag varchar(1))", Collections.emptyMap());
        rows = baki.entity(Row.class);
    }

    @Test
    public void singleEntityRoundTripsAllEncodingsAndQueryEntryPoints() {
        baki.setSqlInterceptor((rawSql, sql, args, info) -> {
            if (sql.startsWith("insert")) {
                assertEquals(Integer.valueOf(1), args.get("numeric_flag"));
                assertEquals("1", args.get("text_flag"));
                assertEquals("Y", args.get("yes_no_flag"));
                assertEquals("Y", args.get("primitive_flag"));
            }
            return sql;
        });
        assertEquals(1, rows.insert().save(row(1, true)));
        assertValues(rows.findById(1L).get(), true);
        assertValues(rows.query().list().get(0), true);
        assertValues(rows.query().top(1).get(0), true);
        assertValues(rows.query().pageable(1, 1).getData().get(0), true);
        assertValues(baki.query("select * from boolean_entries").entities(Row.class).get(0), true);
        assertValues(baki.query("select * from boolean_entries").findFirstEntity(Row.class), true);
        Row read = rows.findById(1L).get();
        read.setNumericFlag(false);
        read.setTextFlag(false);
        read.setYesNoFlag(false);
        read.setPrimitiveFlag(false);
        assertEquals(1, rows.update().save(read));
        DataRow stored = baki.query("select * from boolean_entries").findFirstRow();
        assertEquals(Integer.valueOf(0), stored.getInt("numeric_flag"));
        assertEquals("0", stored.getString("text_flag"));
        assertEquals("N", stored.getString("yes_no_flag"));
        assertEquals("N", stored.getString("primitive_flag"));
        assertValues(rows.findById(1L).get(), false);
    }

    @Test
    public void batchesAndFieldSelectionPreserveFalseAndNull() {
        assertArrayEquals(new int[]{1, 1}, rows.insert().withNullValues()
                .save(Arrays.asList(row(1, true), row(2, false))).getAffectedRows());
        assertArrayEquals(new int[]{1, 1}, rows.update().only(Row::getTextFlag)
                .save(Arrays.asList(row(1, false), row(2, true))).getAffectedRows());
        assertEquals(Boolean.FALSE, rows.findById(1L).get().getTextFlag());
        assertEquals(Boolean.TRUE, rows.findById(1L).get().getYesNoFlag());
        assertEquals(Boolean.TRUE, rows.findById(2L).get().getTextFlag());
        assertArrayEquals(new int[]{1, 1}, rows.update().withNullValues().exclude(Row::getTextFlag)
                .save(Arrays.asList(row(1, null), row(2, null))).getAffectedRows());
        assertNull(rows.findById(1L).get().getNumericFlag());
        assertNull(rows.findById(2L).get().getYesNoFlag());
        assertEquals(Boolean.FALSE, rows.findById(1L).get().getTextFlag());
        assertEquals(Boolean.TRUE, rows.findById(2L).get().getTextFlag());
        assertArrayEquals(new int[]{1, 1}, rows.insert()
                .save(Arrays.asList(row(3, false), row(4, true))).getAffectedRows());
        assertArrayEquals(new int[]{1, 1}, rows.update().withNullValues()
                .save(Arrays.asList(row(3, true), row(4, false))).getAffectedRows());
        assertValues(rows.findById(3L).get(), true);
        assertValues(rows.findById(4L).get(), false);
    }

    @Test
    public void settersAndWhereConditionsUseStorageTypes() {
        AtomicInteger textParameters = new AtomicInteger();
        baki.setSqlInterceptor((rawSql, sql, args, info) -> {
            args.forEach((key, value) -> {
                if (key.startsWith("text_flag") && value != null) {
                    assertTrue(value instanceof String);
                    assertTrue(value.equals("0") || value.equals("1"));
                    textParameters.incrementAndGet();
                }
            });
            return sql;
        });
        assertEquals(1, rows.insert().set(Row::getId, 1L).set(Row::getTextFlag, true)
                .set(Row::getNumericFlag, true).set(Row::getYesNoFlag, true)
                .set(Row::isPrimitiveFlag, false).save());
        assertEquals(1, rows.query().where(w -> w.eq(Row::getTextFlag, true)).count());
        assertTrue(rows.query().where(w -> w.neq(Row::getTextFlag, false)).exists());
        assertTrue(rows.query().where(w -> w.in(Row::getTextFlag, Arrays.asList(false, true))).exists());
        assertTrue(rows.query().where(w -> w.between(Row::getTextFlag, false, true)).exists());
        assertTrue(rows.query().where(w -> w.and(g -> g.eq(Row::getNumericFlag, true)
                .or(inner -> inner.eq(Row::getYesNoFlag, true).eq(Row::getTextFlag, false)))).exists());
        assertEquals(1, rows.update().where(w -> w.eq(Row::getTextFlag, true))
                .set(Row::getTextFlag, false).set(Row::getYesNoFlag, false).save());
        assertEquals(Boolean.FALSE, rows.findById(1L).get().getTextFlag());
        assertEquals(1, rows.delete().where(w -> w.eq(Row::getTextFlag, false)).execute());
        assertTrue(textParameters.get() > 0);
    }

    @Test
    public void nullsRemainNullAndDefaultUpdatesStillSkipThem() {
        rows.insert().withNullValues().save(row(1, null));
        Row read = rows.findById(1L).get();
        assertNull(read.getNumericFlag());
        assertNull(read.getTextFlag());
        assertNull(read.getYesNoFlag());
        assertFalse(read.isPrimitiveFlag());
        rows.update().save(row(1, true));
        rows.update().save(row(1, null));
        assertEquals(Boolean.TRUE, rows.findById(1L).get().getTextFlag());
        rows.update().withNullValues().only(Row::getTextFlag).save(row(1, null));
        assertNull(rows.findById(1L).get().getTextFlag());
        baki.execute("update boolean_entries set primitive_flag = null", Collections.emptyMap());
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> rows.findById(1L));
        assertTrue(error.getMessage(), error.getMessage().contains("primitive_flag"));
    }

    @Test
    public void numericReadingHandlesJdbcNumberTypesWithoutTruncation() {
        for (Number number : new Number[]{(byte) 1, (short) 1, 1, 1L, 1f, 1d, BigInteger.ONE, new BigDecimal("1.000")}) {
            assertEquals(Boolean.TRUE, mapped(DataRow.of("numeric_flag", number)).getNumericFlag());
        }
        assertEquals(Boolean.FALSE, mapped(DataRow.of("numeric_flag", new BigDecimal("0.00"))).getNumericFlag());
        assertEquals(Boolean.TRUE, mapped(DataRow.of("numeric_flag", true)).getNumericFlag());
        for (Object invalid : new Object[]{-1, 2, 0.5, new BigDecimal("1.00000000000000001"), Double.NaN, Double.POSITIVE_INFINITY, "1"}) {
            IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> mapped(DataRow.of("numeric_flag", invalid)));
            assertTrue(error.getMessage(), error.getMessage().contains("numeric_flag"));
            assertTrue(error.getCause().getMessage(), error.getCause().getMessage().contains("NUMERIC"));
        }
    }

    @Test
    public void textReadingRejectsUnknownValuesRatherThanSilentlyReturningFalse() {
        for (Object invalid : new Object[]{"", "2", "true", "Y", " 1", 1}) {
            IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> mapped(DataRow.of("text_flag", invalid)));
            assertTrue(error.getMessage(), error.getMessage().contains("textFlag"));
            assertTrue(error.getMessage(), error.getMessage().contains("text_flag"));
        }
        for (Object invalid : new Object[]{"", "y", "n", " Y", "1", 1}) {
            assertThrows(IllegalStateException.class, () -> mapped(DataRow.of("yes_no_flag", invalid)));
        }
    }

    @Test
    public void configuredFieldsRequireBooleanParameters() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> rows.insert().set(Row::getTextFlag, "1"));
        assertTrue(error.getMessage(), error.getMessage().contains(Row.class.getName()));
        assertTrue(error.getMessage(), error.getMessage().contains("text_flag"));
        assertThrows(IllegalArgumentException.class,
                () -> rows.query().where(w -> w.eq(Row::getTextFlag, 1)).exists());
        assertThrows(IllegalArgumentException.class,
                () -> rows.update().where(w -> w.eq(Row::getId, 1L)).set(Row::getYesNoFlag, "Y"));
    }

    @Test
    public void invalidFieldTypesFailAtMetadataInitializationAndRawQueryMapping() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> baki.entity(InvalidRow.class));
        assertTrue(error.getMessage(), error.getMessage().contains("InvalidRow.flag"));
        assertTrue(error.getMessage(), error.getMessage().contains("java.lang.String"));
        assertThrows(IllegalStateException.class,
                () -> baki.query("select 'Y' as flag").findFirstEntity(InvalidRow.class));
    }

    @Test
    public void fieldsWithoutStorageKeepProviderConversion() {
        AtomicInteger conversions = new AtomicInteger();
        baki.setEntityMetaProvider(new Provider() {
            @Override
            public EntityManager.ColumnMeta columnMeta(Field field) {
                EntityManager.ColumnMeta meta = super.columnMeta(field);
                meta.setBooleanStorage(null);
                return meta;
            }

            @Override
            public Object columnValue(Field field, Object value) {
                conversions.incrementAndGet();
                return "custom".equals(value);
            }
        });
        assertEquals(Boolean.TRUE, baki.query("select 'custom' as text_flag").findFirstEntity(Row.class).getTextFlag());
        assertEquals(1, conversions.get());
        baki.setSqlInterceptor((rawSql, sql, args, info) -> {
            assertEquals(Boolean.TRUE, args.get("text_flag"));
            return sql;
        });
        assertEquals(1, baki.entity(Row.class).insert().save(row(1, true)));
    }

    @Test
    public void storageParticipatesInColumnMetadataEqualityAndHashing() {
        EntityManager.ColumnMeta left = new EntityManager.ColumnMeta("flag");
        EntityManager.ColumnMeta right = new EntityManager.ColumnMeta("flag");
        assertEquals(left, right);
        left.setBooleanStorage(BooleanStorage.ZERO_ONE_STRING);
        assertNotEquals(left, right);
        right.setBooleanStorage(BooleanStorage.ZERO_ONE_STRING);
        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertEquals(1, new HashSet<>(Arrays.asList(left, right)).size());
        right.setBooleanStorage(BooleanStorage.NUMERIC);
        assertNotEquals(left, right);
    }

    private Row mapped(DataRow row) {
        BakiDao probe = new BakiDao(source) {
            @Override
            public Stream<DataRow> executeQueryStream(String sql, Map<String, ?> args) {
                return Stream.of(row);
            }
        };
        probe.setEntityMetaProvider(new Provider());
        return probe.query("select * from boolean_entries").findFirstEntity(Row.class);
    }

    private static Row row(long id, Boolean value) {
        Row row = new Row();
        row.setId(id);
        row.setNumericFlag(value);
        row.setTextFlag(value);
        row.setYesNoFlag(value);
        row.setPrimitiveFlag(Boolean.TRUE.equals(value));
        return row;
    }

    private static void assertValues(Row row, boolean expected) {
        assertEquals(expected, row.getNumericFlag());
        assertEquals(expected, row.getTextFlag());
        assertEquals(expected, row.getYesNoFlag());
        assertEquals(expected, row.isPrimitiveFlag());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface StoredBoolean {
        BooleanStorage value();
    }

    private static class Provider implements EntityManager.EntityMetaProvider {
        @Override
        public String tableName(Class<?> clazz) { return "boolean_entries"; }

        @Override
        public EntityManager.ColumnMeta columnMeta(Field field) {
            EntityManager.ColumnMeta meta = new EntityManager.ColumnMeta(
                    field.getName().replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase(java.util.Locale.ROOT));
            meta.setPrimaryKey(field.getName().equals("id"));
            StoredBoolean annotation = field.getAnnotation(StoredBoolean.class);
            if (annotation != null) {
                meta.setBooleanStorage(annotation.value());
            }
            return meta;
        }

        @Override
        public Object columnValue(Field field, Object value) {
            assertNull("Configured fields must use their storage rule", field.getAnnotation(StoredBoolean.class));
            return ValueUtils.adaptValue(field.getType(), value);
        }
    }

    public static class BaseRow {
        private Long id;
        @StoredBoolean(BooleanStorage.NUMERIC)
        private Boolean numericFlag;
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public Boolean getNumericFlag() { return numericFlag; }
        public void setNumericFlag(Boolean value) { numericFlag = value; }
    }

    public static class Row extends BaseRow {
        @StoredBoolean(BooleanStorage.ZERO_ONE_STRING)
        private Boolean textFlag;
        @StoredBoolean(BooleanStorage.YES_NO)
        private Boolean yesNoFlag;
        @StoredBoolean(BooleanStorage.YES_NO)
        private boolean primitiveFlag;

        public Boolean getTextFlag() { return textFlag; }
        public void setTextFlag(Boolean value) { textFlag = value; }
        public Boolean getYesNoFlag() { return yesNoFlag; }
        public void setYesNoFlag(Boolean value) { yesNoFlag = value; }
        public boolean isPrimitiveFlag() { return primitiveFlag; }
        public void setPrimitiveFlag(boolean value) { primitiveFlag = value; }
    }

    public static class InvalidRow extends BaseRow {
        @StoredBoolean(BooleanStorage.YES_NO)
        private String flag;
        public String getFlag() { return flag; }
        public void setFlag(String flag) { this.flag = flag; }
    }
}
