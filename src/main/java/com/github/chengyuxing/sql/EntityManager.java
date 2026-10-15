package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.PropertyMeta;
import com.github.chengyuxing.common.util.ReflectUtils;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.sql.util.SqlGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Entity manager.
 */
public class EntityManager implements AutoCloseable {
    /**
     * Entity metadata parse provider.
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
    private final SqlGenerator sqlGenerator;
    private volatile EntityMetaProvider entityMetaProvider = new EntityMetaProvider() {
        @Override
        public String tableName(Class<?> clazz) {
            return clazz.getSimpleName();
        }

        @Override
        public EntityManager.ColumnMeta columnMeta(Field field) {
            return new EntityManager.ColumnMeta(field.getName());
        }

        @Override
        public Object columnValue(Field field, Object value) {
            return ValueUtils.adaptValue(field.getType(), value);
        }
    };

    public EntityManager(SqlGenerator sqlGenerator) {
        this.sqlGenerator = sqlGenerator;
    }

    public synchronized <T> EntityMeta getEntityMeta(@NotNull Class<T> clazz) {
        return classInformation.computeIfAbsent(clazz, c -> new EntityMeta(checkTableName(c), checkColumns(c)));
    }

    private String checkTableName(Class<?> clazz) {
        String tableName = entityMetaProvider.tableName(clazz);
        if (StringUtils.isBlank(tableName)) {
            throw new IllegalArgumentException("Missing table name for entity " + clazz.getName());
        }
        return tableName;
    }

    private Map<String, ColumnMeta> checkColumns(Class<?> clazz) {
        Map<String, PropertyMeta> props = ReflectUtils.getBeanPropertyMetas(clazz);
        Map<String, ColumnMeta> columns = new HashMap<>();
        for (PropertyMeta pm : props.values()) {
            if (!pm.hasField()) continue;
            Field field = pm.getField();
            int modifiers = field.getModifiers();
            if (Modifier.isFinal(modifiers) || Modifier.isStatic(modifiers)) {
                continue;
            }
            ColumnMeta columnMeta = entityMetaProvider.columnMeta(field);
            if (columnMeta.isIgnore()) {
                continue;
            }
            String columnName = columnMeta.getName();
            if (StringUtils.isBlank(columnName)) {
                throw new IllegalArgumentException("Missing column name for entity " + clazz.getName() + "." + field.getName());
            }
            if (columns.putIfAbsent(columnName, columnMeta) != null) {
                throw new IllegalArgumentException("Duplicate column '" + columnName + "' in entity " + clazz.getName() +
                        " (field " + field.getName() + ")");
            }
        }
        return columns;
    }

    @Override
    public synchronized void close() {
        classInformation.clear();
    }

    public SqlGenerator getSqlGenerator() {
        return sqlGenerator;
    }

    public EntityMetaProvider getEntityMetaProvider() {
        return entityMetaProvider;
    }

    /**
     * Replace the provider and clear cached metadata. Recreate existing entity executors after this change.
     */
    public synchronized void setEntityMetaProvider(@NotNull EntityMetaProvider entityMetaProvider) {
        this.entityMetaProvider = Objects.requireNonNull(entityMetaProvider, "entityMetaProvider");
        classInformation.clear();
    }

    public class EntityMeta {
        private final String tableName;
        private final Map<String, ColumnMeta> columns;
        private final Map<String, ColumnMeta> insertColumns;
        private final Map<String, ColumnMeta> updateColumns;
        private final ColumnMeta primaryKey;
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
                this.updateById = updateColumns.isEmpty() ? null : genUpdateBy(updateColumns) + idCondition;
                this.deleteById = genDeleteBy() + idCondition;
            }
        }

        public ColumnMeta getPrimaryKey() {
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
            if (updateById == null) {
                throw new IllegalStateException("No updatable columns for table " + tableName);
            }
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

        private ColumnMeta checkPrimaryKey() {
            ColumnMeta primaryKey = null;
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                if (entry.getValue().isPrimaryKey()) {
                    if (primaryKey != null) {
                        throw new IllegalStateException("Multiple primary keys are not supported for table " + tableName +
                                ": " + primaryKey.getName() + ", " + entry.getKey());
                    }
                    primaryKey = entry.getValue();
                }
            }
            if (primaryKey == null) {
                throw new IllegalStateException("Primary key not found for table " + tableName);
            }
            return primaryKey;
        }

        private Map<String, ColumnMeta> collectUpdateColumns() {
            Map<String, ColumnMeta> updateColumns = new HashMap<>();
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                ColumnMeta meta = entry.getValue();
                if (!meta.isPrimaryKey() && meta.isUpdatable()) {
                    updateColumns.put(entry.getKey(), meta);
                }
            }
            return updateColumns;
        }

        private Map<String, ColumnMeta> collectInsertColumns() {
            Map<String, ColumnMeta> insertColumns = new HashMap<>();
            for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
                ColumnMeta meta = entry.getValue();
                if (meta.isInsertable() && meta.getIdGenerateStrategy() == IdGenerateStrategy.NONE) {
                    insertColumns.put(entry.getKey(), meta);
                }
            }
            return insertColumns;
        }

        private String genIdCondition() {
            return sqlGenerator.generateNamedEqualsCondition(primaryKey.getName());
        }

        private String genSelect(Set<String> selectedColumns) {
            Set<String> eCols = columns.keySet();
            Predicate<String> columnSelector = selectedColumns.isEmpty()
                    ? null
                    : selectedColumns::contains;
            return sqlGenerator.generateRecordSelect(tableName, eCols, columnSelector);
        }

        private String genCountSelect() {
            return sqlGenerator.generateCountSelect(tableName);
        }

        private String genInsert(Map<String, ColumnMeta> selectColumns) {
            return sqlGenerator.generateNamedParamInsert(tableName, selectColumns.keySet(), c -> {
                ColumnMeta meta = selectColumns.get(c);
                return columns.containsKey(c) && meta != null && meta.isInsertable();
            });
        }

        private String genUpdateBy(Map<String, ColumnMeta> selectColumns) {
            return sqlGenerator.generateNamedParamUpdateBy(tableName, selectColumns.keySet(), c -> {
                ColumnMeta meta = selectColumns.get(c);
                return columns.containsKey(c) && meta != null && !meta.isPrimaryKey() && meta.isUpdatable();
            });
        }

        private String genDeleteBy() {
            return sqlGenerator.generateDeleteBy(tableName);
        }
    }

    public static class ColumnMeta {
        private String name;
        private boolean primaryKey = false;
        private IdGenerateStrategy idGenerateStrategy = IdGenerateStrategy.NONE;
        private boolean insertable = true;
        private boolean updatable = true;
        /** Enum parameter storage strategy; null uses STRING (the constant name). */
        private ColumnEnumType enumerated;
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

        public IdGenerateStrategy getIdGenerateStrategy() {
            return idGenerateStrategy;
        }

        public void setIdGenerateStrategy(IdGenerateStrategy idGenerateStrategy) {
            if (idGenerateStrategy != null) {
                this.idGenerateStrategy = idGenerateStrategy;
            }
        }

        public ColumnEnumType getEnumerated() {
            return enumerated;
        }

        public void setEnumerated(ColumnEnumType enumerated) {
            this.enumerated = enumerated;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof ColumnMeta)) return false;

            ColumnMeta meta = (ColumnMeta) o;
            return isPrimaryKey() == meta.isPrimaryKey() && isInsertable() == meta.isInsertable() && isUpdatable() == meta.isUpdatable() && isIgnore() == meta.isIgnore() && getName().equals(meta.getName()) && getIdGenerateStrategy() == meta.getIdGenerateStrategy()
                    && getEnumerated() == meta.getEnumerated();
        }

        @Override
        public int hashCode() {
            int result = getName().hashCode();
            result = 31 * result + Boolean.hashCode(isPrimaryKey());
            result = 31 * result + getIdGenerateStrategy().hashCode();
            result = 31 * result + Boolean.hashCode(isInsertable());
            result = 31 * result + Boolean.hashCode(isUpdatable());
            result = 31 * result + Boolean.hashCode(isIgnore());
            result = 31 * result + Objects.hashCode(getEnumerated());
            return result;
        }
    }

    public enum IdGenerateStrategy {
        /**
         * Generate by manual
         */
        NONE,
        /**
         * Generate by database auto increment or sequence
         */
        IDENTITY
    }

    public enum ColumnEnumType {
        ORDINAL,
        STRING,
    }
}
