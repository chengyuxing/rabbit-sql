package entity;

import com.github.chengyuxing.sql.Baki;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityManager<T> {
    private final Baki baki;
    private final Class<T> entityClass;
    private final Map<Class<T>, EntityMeta> objectMap = new HashMap<>();
    private String tableName;
    private String fields;

    public EntityManager(Baki baki, Class<T> entityClass) {
        this.baki = baki;
        this.entityClass = entityClass;
    }

    @SafeVarargs
    public final Query<T> query(FieldFunction<T>... fields) {
        return new Query<>(entityClass, fields);
    }

    public int delete(T entity) {
        return 0;
    }

    public int update(T entity) {
        return 0;
    }

    public int insert(T entity) {
        return 0;
    }

    static class EntityMeta {
        private String tableName;
        private String pk;
        private List<FieldMeta> fieldMeta;
    }

    static class FieldMeta {
        private final String name;
        private boolean insert = true;
        private boolean update = true;
        private boolean nullable = true;

        FieldMeta(String name) {
            this.name = name;
        }
    }
}
