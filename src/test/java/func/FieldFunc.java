package func;

import java.io.Serializable;

@FunctionalInterface
public interface FieldFunc<T> extends Serializable {
    Object get(T t);
}
