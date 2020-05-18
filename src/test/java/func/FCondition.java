package func;

import rabbit.sql.types.Order;
import rabbit.sql.types.Param;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class FCondition<T> {
    private String where;
    private List<String> orderByes;
    private List<String> ands;
    private List<String> ors;
    private Map<String, Param> params = new HashMap<>();

    private FCondition() {

    }

    public static <T> FCondition<T> builder() {
        return new FCondition<T>();
    }

    public FCondition<T> where(FFilter<T> filter) {
        where = " where" + filter.getSql();
        if (filter.value() != null)
            params.put(filter.field(), Param.IN(filter.value()));
        return this;
    }

    public FCondition<T> orderBy(FieldFunc<T> field) {
        orderBy(field, Order.ASC);
        return this;
    }

    public FCondition<T> orderBy(FieldFunc<T> field, Order order) {
        if (orderByes == null)
            orderByes = new ArrayList<>();
        try {
            String _field = BeanUtil.convert2fieldName(field);
            orderByes.add(String.format("%s %s", _field, order.getValue()));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return this;
    }

    public FCondition<T> and(FFilter<T> filter, FFilter<T>... more) {
        if (ands == null)
            ands = new ArrayList<>();
        if (more.length == 0) {
            if (filter.value() != null)
                params.put(filter.field(), Param.IN(filter.value()));
            ands.add(filter.getSql());
            return this;
        }
        List<FFilter<T>> filters = new ArrayList<>(1 + more.length);
        filters.add(0, filter);
        filters.addAll(1, Arrays.asList(more));
        String andGroup = filters.stream()
                .map(f -> {
                    if (f.value() != null)
                        params.put(f.field(), Param.IN(f.value()));
                    return f.getSql();
                }).collect(Collectors.joining(" and "));
        ands.add(String.format("(%s)", andGroup));
        return this;
    }

    public FCondition<T> or(FFilter<T> filter, FFilter<T>... more) {
        if (ors == null)
            ors = new ArrayList<>();
        if (more.length == 0) {
            if (filter.value() != null)
                params.put(filter.field(), Param.IN(filter.value()));
            ors.add(filter.getSql());
            return this;
        }
        List<FFilter<T>> filters = new ArrayList<>(1 + more.length);
        filters.add(0, filter);
        filters.addAll(1, Arrays.asList(more));
        String andGroup = filters.stream()
                .map(f -> {
                    if (f.value() != null)
                        params.put(f.field(), Param.IN(f.value()));
                    return f.getSql();
                }).collect(Collectors.joining(" or "));
        ors.add(String.format("(%s)", andGroup));
        return this;
    }

    @Override
    public String toString() {
        String _ands = "",
                _ors = "",
                _orders = "";
        if (ands != null) {
            _ands = " and " + String.join(" and ", ands);
        }
        if (ors != null) {
            _ors = " or " + String.join(" or ", ors);
        }
        if (orderByes != null) {
            _orders = " order by " + String.join(", ", orderByes);
        }
        return where + _ands + _ors + _orders;
    }

    /**
     * 获取条件拼装器中的参数
     *
     * @return 参数
     */
    public Map<String, Param> getParams() {
        FFilter.integer.set(0);
        return params;
    }
}
