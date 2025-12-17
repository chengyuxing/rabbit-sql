package baki.entityExecutor;

import com.github.chengyuxing.sql.EntityManager;

import javax.persistence.*;
import java.lang.reflect.Field;

public class MyEntityMetaParser implements EntityManager.EntityMetaProvider {

    @Override
    public String tableName(Class<?> clazz) {
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new IllegalStateException(clazz.getName() + " must be annotated with @" + Entity.class.getSimpleName());
        }
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

    @Override
    public EntityManager.ColumnMeta columnMeta(Field field) {
        EntityManager.ColumnMeta columnMeta = new EntityManager.ColumnMeta(field.getName());
        if (field.isAnnotationPresent(Column.class)) {
            Column column = field.getAnnotation(Column.class);
            columnMeta.setName(column.name());
            columnMeta.setInsertable(column.insertable());
            columnMeta.setUpdatable(column.updatable());
        }
        columnMeta.setPrimaryKey(field.isAnnotationPresent(Id.class));
        columnMeta.setIgnore(field.isAnnotationPresent(Transient.class));
        return columnMeta;
    }

    @Override
    public Object columnValue(Field field, Object value) {
        if (field.getType() == LazyReference.class) {
            return (LazyReference<Object>) () -> {
                System.out.println("Hello world!!!");
                return value;
            };
        }
        return value;
    }
}
