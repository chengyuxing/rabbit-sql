package baki.entityExecutor;

import com.github.chengyuxing.common.tuple.Quadruple;
import func.FieldFunc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public abstract class Where<T> {
    protected final List<Quadruple<String, FieldFunc<T>, String, Object>> criteria = new ArrayList<>();

    public Where(FieldFunc<T> field, String op, Object value) {
        _addCriteria("", field, op, value);
    }

    public <V> Where<T> and(FieldFunc<T> field, String op, V value, Predicate<V> predicate) {
        if (predicate.test(value)) {
            return _addCriteria("and", field, op, value);
        }
        return this;
    }

    public <V> Where<T> and(FieldFunc<T> field, String op, V value) {
        return _addCriteria("and", field, op, value);
    }

    public Where<T> or(FieldFunc<T> field, String op, Object value) {
        return _addCriteria("or", field, op, value);
    }

    public Where<T> or(FieldFunc<T> field, String op, FieldFunc<T> value) {
        return _addCriteria("or", field, op, value);
    }

    public <V> Where<T> or(FieldFunc<T> field, String op, V value, Predicate<V> predicate) {
        if (predicate.test(value)) {
            return _addCriteria("or", field, op, value);
        }
        return this;
    }

    Where<T> _addCriteria(String prefix, FieldFunc<T> field, String op, Object value) {
        criteria.add(Quadruple.of(prefix, field, op, value));
        return this;
    }

    public abstract EntityQuery<T> query(FieldFunc<T>... fields);

    public abstract int save(T entity);

    public abstract int save(Collection<T> entities);

    public abstract int delete();
}
