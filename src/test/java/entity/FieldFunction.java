package entity;

import java.io.Serializable;

@FunctionalInterface
public interface FieldFunction<T> extends Serializable {
    Object get(T entity);
}
