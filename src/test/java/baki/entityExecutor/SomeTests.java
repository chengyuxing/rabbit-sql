package baki.entityExecutor;

import baki.entity.User;
import baki.entityExecutor.annotation.Col;
import baki.entityExecutor.annotation.Table;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.Args;
import func.BeanUtil;
import func.FieldFunc;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SomeTests {
    @Test
    public void test1() {
        User user = new User();
        user.setAge(21);
        user.setDt(LocalDateTime.now());
        user.setName("cyx");

        EntityExecutorImpl<User> executor = new EntityExecutorImpl<>(User.class);
        executor.where(User::getAddress, "=", "kunming")
                .or(User::getId, "=", 12, Objects::nonNull)
                .or(User::getAge, "=", User::getAge)
                .save(user);
    }

    static class EntityMeta {
        private final String schema;
        private final String table;
        private final Map<String, String> fieldMap = new HashMap<>();

        EntityMeta(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        public void addField(String field, String colName) {
            fieldMap.put(field, colName);
        }

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public Map<String, String> getFieldMap() {
            return fieldMap;
        }
    }

    static class EntityExecutorImpl<T> extends EntityExecutor<T> {
        private static final Map<Class<?>, EntityMeta> entityMetas = new HashMap<>();
        private final ReentrantLock lock = new ReentrantLock();

        protected EntityExecutorImpl(Class<T> entityClass) {
            super(entityClass);
        }

        EntityMeta getMeta(Class<?> entityClass) {
            if (entityMetas.containsKey(entityClass)) {
                return entityMetas.get(entityClass);
            }
            lock.lock();
            try {
                if (!entityClass.isAnnotationPresent(Table.class)) {
                    throw new IllegalArgumentException("@Table not found.");
                }
                Table tableAnno = entityClass.getAnnotation(Table.class);
                EntityMeta meta = new EntityMeta(tableAnno.schema(), tableAnno.value());
                Field[] fields = entityClass.getDeclaredFields();
                for (Field field : fields) {
                    if (Modifier.isFinal(field.getModifiers())) {
                        continue;
                    }
                    if (!field.isAnnotationPresent(Col.class)) {
                        continue;
                    }
                    Col col = field.getAnnotation(Col.class);
                    meta.addField(field.getName(), col.value());
                }
                entityMetas.put(entityClass, meta);
                return meta;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Where<T> where(FieldFunc<T> field, String op, Object value) {
            return new Where<T>(field, op, value) {
                @Override
                public EntityQuery<T> query(FieldFunc<T>... fields) {
                    return null;
                }

                @Override
                public int save(T entity) {
                    String where = criteria.stream().map(q -> {
                        String separator = q.getItem1();
                        try {
                            String field = BeanUtil.convert2fieldName(q.getItem2());
                            String op = q.getItem3();
                            Object value = q.getItem4() instanceof FieldFunc ? ':' + BeanUtil.convert2fieldName((FieldFunc<?>) q.getItem4()) : q.getItem4();
                            return separator + " " + field + " " + op + " " + value;
                        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.joining("\n\t"));
                    EntityMeta meta = getMeta(entityClass);
                    String temp = "update ${table}\nset ${sets}\nwhere ${where}";
                    StringJoiner sb = new StringJoiner(", \n\t");
                    meta.getFieldMap().forEach((k, v) -> {
                        String field = v.isEmpty() ? k : v;
                        sb.add(field + " = :" + field);
                    });
                    String update = StringUtils.FMT.format(temp,
                            Args.of(
                                    "table", meta.getSchema() + '.' + meta.getTable(),
                                    "sets", sb,
                                    "where", where
                            ));
                    System.out.println(update);
                    return 0;
                }

                @Override
                public int save(Collection<T> entities) {
                    return 0;
                }

                @Override
                public int delete() {
                    return 0;
                }
            };
        }

        @Override
        public int save(T entity) {
            return 0;
        }

        @Override
        public int save(Collection<? extends T> entities) {
            return 0;
        }
    }
}
