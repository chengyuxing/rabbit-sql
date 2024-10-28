package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.sql.dsl.type.StandardOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

public class EntityManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EntityManager.class);
    private final Map<Class<?>, EntityMeta> classInformation = new ConcurrentHashMap<>();
    private final char namedParamPrefix;

    public EntityManager(char namedParamPrefix) {
        if (namedParamPrefix == ' ') {
            throw new IllegalArgumentException("prefix char cannot be empty.");
        }
        this.namedParamPrefix = namedParamPrefix;
    }

    public <T> EntityMeta getEntityMeta(Class<T> clazz) {
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new IllegalStateException(clazz.getName() + " must be annotated with @" + Entity.class.getSimpleName());
        }
        return classInformation.computeIfAbsent(clazz, c -> new EntityMeta(namedParamPrefix, checkTableName(c), checkColumns(c)));
    }

    public String getTableName(Class<?> clazz) {
        return getEntityMeta(clazz).getTableName();
    }

    public Map<String, ColumnMeta> getColumns(Class<?> clazz) {
        return getEntityMeta(clazz).getColumns();
    }

    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }

    public static final class EntityMeta {
        private final char namedParamPrefix;
        private final String tableName;
        private final String primaryKey;
        private final Map<String, ColumnMeta> columns;
        private final String select;
        private final String countSelect;
        private final String existsSelect;
        private final String insert;
        private final String update;
        private final String delete;

        public EntityMeta(char namedParamPrefix, String tableName, Map<String, ColumnMeta> columns) {
            this.namedParamPrefix = namedParamPrefix;
            this.tableName = tableName;
            this.columns = columns;
            this.primaryKey = checkPrimaryKey();
            this.select = genSelect();
            this.countSelect = genCountSelect();
            this.existsSelect = genExistSelect();
            this.insert = genInsert();
            this.update = genUpdate();
            this.delete = genDelete();
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public Map<String, ColumnMeta> getColumns() {
            return columns;
        }

        public String getTableName() {
            return tableName;
        }

        public String getSelect() {
            return select;
        }

        public String getCountSelect() {
            return countSelect;
        }

        public String getExistsSelect() {
            return existsSelect;
        }

        public String getInsert() {
            return insert;
        }

        public String getUpdate() {
            return update;
        }

        public String getDelete() {
            return delete;
        }

        private String checkPrimaryKey() {
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                if (Objects.nonNull(entry.getValue().getId())) {
                    return entry.getKey();
                }
            }
            throw new IllegalStateException("Primary Key not found");
        }

        private String genSelect() {
            return "select " + String.join(", ", columns.keySet()) + " from " + tableName;
        }

        private String genCountSelect() {
            return "select count(*) from " + tableName;
        }

        private String genExistSelect() {
            return "select 1 from " + tableName;
        }

        private String genInsert() {
            StringJoiner f = new StringJoiner(", ");
            StringJoiner h = new StringJoiner(", ");
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                Column column = entry.getValue().getColumn();
                if (Objects.isNull(column) || column.insertable()) {
                    f.add(entry.getKey());
                    h.add(namedParamPrefix + entry.getKey());
                }
            }
            return "insert into " + tableName + "(" + f + ") values (" + h + ")";
        }

        private String genUpdate() {
            StringJoiner sets = new StringJoiner(",\n\t");
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                Column column = entry.getValue().getColumn();
                if (Objects.isNull(entry.getValue().getId()) && (Objects.isNull(column) || column.updatable())) {
                    sets.add(entry.getKey() + StandardOperator.EQ + namedParamPrefix + entry.getKey());
                }
            }
            return "update " + tableName + "\nset " + sets;
        }

        private String genDelete() {
            return "delete from " + tableName;
        }
    }

    public static final class ColumnMeta {
        private final Method getter;
        private final Method setter;
        private final Class<?> type;
        private final Id id;
        private Column column;

        public ColumnMeta(Method getter, Method setter, Class<?> type, Id id) {
            this.getter = getter;
            this.setter = setter;
            this.type = type;
            this.id = id;
        }

        public Method getGetter() {
            return getter;
        }

        public Class<?> getType() {
            return type;
        }

        public Method getSetter() {
            return setter;
        }

        public Id getId() {
            return id;
        }

        public Column getColumn() {
            return column;
        }

        public void setColumn(Column column) {
            this.column = column;
        }
    }

    private String checkTableName(Class<?> clazz) {
        String tableName = clazz.getSimpleName().toLowerCase();
        if (clazz.isAnnotationPresent(Table.class)) {
            Table table = clazz.getAnnotation(Table.class);
            if (!table.name().isEmpty()) {
                tableName = table.name();
            }
            if (!table.schema().isEmpty()) {
                tableName = table.schema() + "." + tableName;
            }
        }
        return tableName;
    }

    private Map<String, ColumnMeta> checkColumns(Class<?> clazz) {
        Map<String, ColumnMeta> columns = new HashMap<>();
        try {
            for (Field field : clazz.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                if (Modifier.isFinal(modifiers) || Modifier.isStatic(modifiers)) {
                    continue;
                }
                Method getter = ReflectUtil.getGetMethod(clazz, field);
                Method setter = ReflectUtil.getSetMethod(clazz, field);
                ColumnMeta columnMeta = new ColumnMeta(getter, setter, field.getType(), field.getAnnotation(Id.class));
                String columnName = field.getName();
                if (field.isAnnotationPresent(Column.class)) {
                    columnMeta.setColumn(field.getAnnotation(Column.class));
                    String name = field.getAnnotation(Column.class).name().trim();
                    if (!name.isEmpty()) {
                        columnName = name;
                    }
                }
                columns.put(columnName, columnMeta);
            }
            return columns;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        classInformation.clear();
    }
}
