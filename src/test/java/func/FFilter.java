package func;

import com.github.chengyuxing.common.tuple.Triple;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.chengyuxing.sql.utils.SqlUtil.SEP;

public class FFilter<T> {
    static final AtomicInteger integer = new AtomicInteger();
    private final Triple<String, String, Object> filter;

    private static <T> String getField(String field) {
        int idx = integer.getAndIncrement();
        return String.format("%s%s%s", field, SEP, idx);
    }

    FFilter(Triple<String, String, Object> filter) {
        this.filter = filter;
    }

    public static <T> FFilter<T> eq(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s = :%s", field, _field), _field, value));
    }

    public static <T> FFilter<T> neq(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s != :%s", field, _field), _field, value));
    }

    public static <T> FFilter<T> gt(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s > :%s", field, _field), _field, value));
    }

    public static <T> FFilter<T> lt(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s < :%s", field, _field), _field, value));
    }

    public static <T> FFilter<T> gtEq(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s >= :%s", field, _field), _field, value));
    }

    public static <T> FFilter<T> ltEq(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s <= :%s", field, _field), _field, value));
    }

    public static <T> FFilter<T> like(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s like :%s", field, _field), _field, value));
    }

    public static <T> FFilter<T> notLike(FieldFunc<T> fieldFunc, Object value) {
        String field = getFieldName(fieldFunc);
        String _field = getField(field);
        return new FFilter<>(Triple.of(String.format(" %s not like :%s", _field, _field), field, value));
    }

    public static <T> FFilter<T> isNotNull(FieldFunc<T> fieldFunc) {
        String field = getFieldName(fieldFunc);
        return new FFilter<>(Triple.of(String.format(" %s is not null", field), field, null));
    }

    public static <T> FFilter<T> isNull(FieldFunc<T> fieldFunc) {
        String field = getFieldName(fieldFunc);
        return new FFilter<>(Triple.of(String.format(" %s is null", field), field, null));
    }

    public String getSql() {
        return filter.getItem1();
    }

    public Object value() {
        return filter.getItem3();
    }

    public String field() {
        return filter.getItem2();
    }

    private static <T> String getFieldName(FieldFunc<T> fieldFunc) {
        try {
            return BeanUtil.convert2fieldName(fieldFunc);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            return "";
        }
    }
}
