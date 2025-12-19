package com.github.chengyuxing.sql;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Entity manager.
 */
public class EntityManager implements AutoCloseable {
    /**
     * Entity meta data parse provider.
     */
    public interface EntityMetaProvider {
        /**
         * Provide a class to parse for a table name.
         *
         * @param clazz entity class
         * @return table name
         */
        String tableName(Class<?> clazz);

        /**
         * Provide an entity field to mapping for database column metadata.
         *
         * @param field entity field
         * @return database column metadata
         */
        ColumnMeta columnMeta(Field field);

        /**
         * Provide an entity field to mapping for database column value.
         *
         * @param field entity field
         * @param value database column value
         * @return the suitable value for field
         */
        Object columnValue(Field field, Object value);
    }

    private final Map<Class<?>, EntityMeta> classInformation = new ConcurrentHashMap<>();
    private final char namedParamPrefix;
    private EntityMetaProvider entityMetaProvider;

    public EntityManager(char namedParamPrefix) {
        if (namedParamPrefix == ' ') {
            throw new IllegalArgumentException("Prefix char cannot be empty.");
        }
        this.namedParamPrefix = namedParamPrefix;
    }

    public <T> EntityMeta getEntityMeta(@NotNull Class<T> clazz) {
        return classInformation.computeIfAbsent(clazz, c -> new EntityMeta(checkTableName(c), checkColumns(c)));
    }

    private String checkTableName(Class<?> clazz) {
        return entityMetaProvider.tableName(clazz);
    }

    private Map<String, ColumnMeta> checkColumns(Class<?> clazz) {
        Map<String, ColumnMeta> columns = new HashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isFinal(modifiers) || Modifier.isStatic(modifiers)) {
                continue;
            }
            ColumnMeta columnMeta = entityMetaProvider.columnMeta(field);
            if (columnMeta.isIgnore()) {
                continue;
            }
            columns.put(columnMeta.getName(), columnMeta);
        }
        return columns;
    }

    @Override
    public void close() throws Exception {
        classInformation.clear();
    }

    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }

    public EntityMetaProvider getEntityMetaProvider() {
        return entityMetaProvider;
    }

    public void setEntityMetaProvider(@NotNull EntityMetaProvider entityMetaProvider) {
        this.entityMetaProvider = entityMetaProvider;
    }

    public class EntityMeta {
        private final String tableName;
        private final Map<String, ColumnMeta> columns;
        private final Map<String, ColumnMeta> insertColumns;
        private final Map<String, ColumnMeta> updateColumns;
        private final String primaryKey;
        private final String idCondition;
        private final String select;
        private final String countSelect;
        private final String insert;
        private final String updateById;
        private final String deleteById;

        public EntityMeta(String tableName, Map<String, ColumnMeta> columns) {
            this.tableName = tableName;
            this.columns = columns;
            {
                this.primaryKey = checkPrimaryKey();
                this.insertColumns = collectInsertColumns();
                this.updateColumns = collectUpdateColumns();
                this.idCondition = genIdCondition();
                this.select = genSelect(Collections.emptySet());
                this.countSelect = genCountSelect();
                this.insert = genInsert(insertColumns);
                this.updateById = genUpdateBy(updateColumns) + idCondition;
                this.deleteById = genDeleteBy() + idCondition;
            }
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public @Unmodifiable Map<String, ColumnMeta> getColumns() {
            return Collections.unmodifiableMap(columns);
        }

        public @Unmodifiable Map<String, ColumnMeta> getUpdateColumns() {
            return Collections.unmodifiableMap(updateColumns);
        }

        public @Unmodifiable Map<String, ColumnMeta> getInsertColumns() {
            return Collections.unmodifiableMap(insertColumns);
        }

        public String getTableName() {
            return tableName;
        }

        public String getSelect() {
            return select;
        }

        public String getSelect(Set<String> includes) {
            return genSelect(includes);
        }

        public String getCountSelect() {
            return countSelect;
        }

        public String getIdCondition() {
            return idCondition;
        }

        public String getInsert() {
            return insert;
        }

        public String getInsert(Map<String, ColumnMeta> columns) {
            return genInsert(columns);
        }

        public String getUpdateById() {
            return updateById;
        }

        public String getUpdateBy(Map<String, ColumnMeta> columns) {
            return genUpdateBy(columns);
        }

        public String getDeleteById() {
            return deleteById;
        }

        public String getDeleteBy() {
            return genDeleteBy();
        }

        private String checkPrimaryKey() {
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                if (entry.getValue().isPrimaryKey()) {
                    return entry.getKey();
                }
            }
            throw new IllegalStateException("Primary key not found");
        }

        private Map<String, ColumnMeta> collectUpdateColumns() {
            Map<String, ColumnMeta> updateColumns = new HashMap<>();
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                if (!entry.getValue().isPrimaryKey() && entry.getValue().isUpdatable()) {
                    updateColumns.put(entry.getKey(), entry.getValue());
                }
            }
            return updateColumns;
        }

        private Map<String, ColumnMeta> collectInsertColumns() {
            Map<String, ColumnMeta> insertColumns = new HashMap<>();
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                if (entry.getValue().isInsertable()) {
                    insertColumns.put(entry.getKey(), entry.getValue());
                }
            }
            return insertColumns;
        }

        private String genIdCondition() {
            return primaryKey + " = " + namedParamPrefix + primaryKey;
        }

        private String genSelect(Set<String> selectedColumns) {
            String delimiter = columns.size() > 7 ? ",\n\t" : ", ";
            String fields;
            if (selectedColumns.isEmpty()) {
                fields = String.join(delimiter, columns.keySet());
            } else {
                StringJoiner sb = new StringJoiner(delimiter);
                for (String sc : selectedColumns) {
                    if (columns.containsKey(sc)) {
                        sb.add(sc);
                    }
                }
                fields = sb.toString();
            }
            return "select " + fields + "\nfrom " + tableName;
        }

        private String genCountSelect() {
            return "select count(*)\nfrom " + tableName;
        }

        private String genInsert(Map<String, ColumnMeta> selectColumns) {
            if (selectColumns.isEmpty()) {
                return "insert into " + tableName + " default values";
            }
            StringJoiner f = new StringJoiner(", ");
            StringJoiner h = new StringJoiner(", ");
            for (Map.Entry<String, ColumnMeta> entry : selectColumns.entrySet()) {
                if (columns.containsKey(entry.getKey()) && entry.getValue().isInsertable()) {
                    f.add(entry.getKey());
                    h.add(namedParamPrefix + entry.getKey());
                }
            }
            return "insert into " + tableName + "(" + f + ") values (" + h + ")";
        }

        private String genUpdateBy(Map<String, ColumnMeta> selectColumns) {
            StringJoiner sets = new StringJoiner(",\n\t");
            for (Map.Entry<String, ColumnMeta> entry : selectColumns.entrySet()) {
                if (columns.containsKey(entry.getKey()) && !entry.getValue().isPrimaryKey() && entry.getValue().isUpdatable()) {
                    sets.add(entry.getKey() + " = " + namedParamPrefix + entry.getKey());
                }
            }
            return "update " + tableName + "\nset " + sets + "\nwhere ";
        }

        private String genDeleteBy() {
            return "delete from " + tableName + " where ";
        }
    }

    public static class ColumnMeta {
        private String name;
        private boolean primaryKey = false;
        private boolean insertable = true;
        private boolean updatable = true;
        private boolean ignore = false;

        public ColumnMeta(@NotNull String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
        }

        public boolean isInsertable() {
            return insertable;
        }

        public void setInsertable(boolean insertable) {
            this.insertable = insertable;
        }

        public boolean isUpdatable() {
            return updatable;
        }

        public void setUpdatable(boolean updatable) {
            this.updatable = updatable;
        }

        public boolean isIgnore() {
            return ignore;
        }

        public void setIgnore(boolean ignore) {
            this.ignore = ignore;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof ColumnMeta)) return false;

            ColumnMeta that = (ColumnMeta) o;
            return isPrimaryKey() == that.isPrimaryKey() && isInsertable() == that.isInsertable() && isUpdatable() == that.isUpdatable() && isIgnore() == that.isIgnore() && getName().equals(that.getName());
        }

        @Override
        public int hashCode() {
            int result = getName().hashCode();
            result = 31 * result + Boolean.hashCode(isPrimaryKey());
            result = 31 * result + Boolean.hashCode(isInsertable());
            result = 31 * result + Boolean.hashCode(isUpdatable());
            result = 31 * result + Boolean.hashCode(isIgnore());
            return result;
        }
    }
}
