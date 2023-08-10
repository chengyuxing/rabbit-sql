package baki.entityExecutor;

import func.FieldFunc;

import java.util.Collection;

public abstract class EntityExecutor<T> {
    protected final Class<T> entityClass;

    protected EntityExecutor(Class<T> entityClass) {
        this.entityClass = entityClass;
    }

    public abstract Where<T> where(FieldFunc<T> field, String op, Object value);

    public abstract int save(T entity);

    public abstract int save(Collection<? extends T> entities);

    public static void main(String[] args) {
//        EntityExecutor<User> entityExecutor = new EntityExecutor<>(User.class);
//        entityExecutor.where(User::getAddress, "=", 10)
//                .and(User::getId, "<>", 1, Objects::nonNull)
//                .or(User::getName, "==", "cyx")
//                .query(User::getAddress, User::getAge, User::getAge, User::getDt)
//                .forEach(System.out::println);
    }
}
