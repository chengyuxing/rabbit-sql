package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.sql.dsl.types.StandardOperator;
import org.jetbrains.annotations.NotNull;

import jakarta.persistence.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Entity manager.
 *
 * @see Entity @Entity
 * @see Table @Table
 * @see Id @Id
 * @see Column @Column
 * @see Transient @Transient
 */
public class EntityManager implements AutoCloseable {
    private final Map<Class<?>, EntityMeta> classInformation = new ConcurrentHashMap<>();
    private final char namedParamPrefix;

    public EntityManager(char namedParamPrefix) {
        if (namedParamPrefix == ' ') {
            throw new IllegalArgumentException("prefix char cannot be empty.");
        }
        this.namedParamPrefix = namedParamPrefix;
    }

    public <T> EntityMeta getEntityMeta(@NotNull Class<T> clazz) {
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new IllegalStateException(clazz.getName() + " must be annotated with @" + Entity.class.getSimpleName());
        }
        return classInformation.computeIfAbsent(clazz, c -> new EntityMeta(namedParamPrefix, checkTableName(c), checkColumns(c)));
    }

    public String getTableName(@NotNull Class<?> clazz) {
        return getEntityMeta(clazz).getTableName();
    }

    public Map<String, ColumnMeta> getColumns(@NotNull Class<?> clazz) {
        return getEntityMeta(clazz).getColumns();
    }

    public char getNamedParamPrefix() {
        return namedParamPrefix;
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
                if (field.isAnnotationPresent(Transient.class)) {
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

    public static final class EntityMeta {
        private final char namedParamPrefix;
        private final String tableName;
        private final String primaryKey;
        private final String whereById;
        private final Map<String, ColumnMeta> columns;
        private final Set<String> updateColumns;
        private final Set<String> insertColumns;
        private final String select;
        private final String countSelect;
        private final String existsSelect;
        private final String insert;
        private final String updateBy;
        private final String updateById;
        private final String deleteBy;
        private final String deleteById;

        public EntityMeta(char namedParamPrefix, String tableName, Map<String, ColumnMeta> columns) {
            this.namedParamPrefix = namedParamPrefix;
            this.tableName = tableName;
            this.columns = columns;
            this.primaryKey = checkPrimaryKey();
            this.whereById = genWhereById();
            this.updateColumns = genUpdateSetColumns();
            this.insertColumns = genInsertColumns();
            this.select = genSelect(this.columns.keySet());
            this.countSelect = genCountSelect();
            this.existsSelect = genExistSelect();
            this.insert = genInsert();
            this.updateBy = genUpdate("");
            this.updateById = genUpdate(whereById);
            this.deleteBy = genDelete("");
            this.deleteById = genDelete(whereById);
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public String getWhereById() {
            return whereById;
        }

        public Map<String, ColumnMeta> getColumns() {
            return columns;
        }

        public Set<String> getInsertColumns() {
            return insertColumns;
        }

        public Set<String> getUpdateColumns() {
            return updateColumns;
        }

        public String getTableName() {
            return tableName;
        }

        public String getSelect() {
            return select;
        }

        public String getSelect(Set<String> columns) {
            return genSelect(columns);
        }

        public String getCountSelect() {
            return countSelect;
        }

        public String getExistsSelect(String where) {
            return "select exists (" + existsSelect + where + ")";
        }

        public String getInsert() {
            return insert;
        }

        public String getUpdateBy(String where) {
            return updateBy + where;
        }

        public String getUpdateById() {
            return updateById;
        }

        public String getDeleteById() {
            return deleteById;
        }

        public String getDeleteBy(String where) {
            return deleteBy + where;
        }

        private String checkPrimaryKey() {
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                if (Objects.nonNull(entry.getValue().getId())) {
                    return entry.getKey();
                }
            }
            throw new IllegalStateException("Primary Key not found");
        }

        private String genWhereById() {
            return "\nwhere " + primaryKey + StandardOperator.EQ.padWithSpace() + namedParamPrefix + primaryKey;
        }

        private String genSelect(Set<String> columns) {
            String delimiter = columns.size() > 7 ? ",\n\t" : ", ";
            return "select " + String.join(delimiter, columns) + "\nfrom " + tableName;
        }

        private String genCountSelect() {
            return "select count(*)\nfrom " + tableName;
        }

        private String genExistSelect() {
            return "select 1 from " + tableName;
        }

        private Set<String> genInsertColumns() {
            Set<String> insertColumns = new HashSet<>();
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                Column column = entry.getValue().getColumn();
                if (Objects.isNull(column) || column.insertable()) {
                    insertColumns.add(entry.getKey());
                }
            }
            return insertColumns;
        }

        private String genInsert() {
            StringJoiner f = new StringJoiner(", ");
            StringJoiner h = new StringJoiner(", ");
            for (String column : insertColumns) {
                f.add(column);
                h.add(namedParamPrefix + column);
            }
            return "insert into " + tableName + "(" + f + ") values (" + h + ")";
        }

        private Set<String> genUpdateSetColumns() {
            Set<String> setColumns = new HashSet<>();
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                if (Objects.nonNull(entry.getValue().getId())) {
                    continue;
                }
                Column column = entry.getValue().getColumn();
                if (Objects.isNull(column) || column.updatable()) {
                    setColumns.add(entry.getKey());
                }
            }
            return setColumns;
        }

        private String genUpdate(String where) {
            StringJoiner sets = new StringJoiner(",\n\t");
            for (String column : updateColumns) {
                sets.add(column + StandardOperator.EQ.padWithSpace() + namedParamPrefix + column);
            }
            return "update " + tableName + "\nset " + sets + where;
        }

        private String genDelete(String where) {
            return "delete from " + tableName + where;
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
}
