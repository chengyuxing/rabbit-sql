package entity;

import func.BeanUtil;

import java.util.ArrayList;
import java.util.List;

public class Query<T> {
    private final Class<T> clazz;
    private final FieldFunction<T>[] fields;

    private String where;
    private List<String> filters = new ArrayList<>();

    @SafeVarargs
    public Query(Class<T> clazz, FieldFunction<T>... fields) {
        this.clazz = clazz;
        this.fields = fields;
    }

    public Query<T> where(FieldFunction<T> field, String op, String value) {
        if (this.where == null) {
            this.where = "where";
        }
//        this.filters.add(BeanUtil.convert2fieldName(field)+op + value);
        return this;
    }

}
