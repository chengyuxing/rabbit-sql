package org.rabbit.sql.dao;

import org.rabbit.common.tuple.Pair;
import org.rabbit.sql.support.ICondition;
import org.rabbit.sql.support.IFilter;
import org.rabbit.sql.types.Order;
import org.rabbit.sql.types.Param;
import org.rabbit.sql.types.ValueWrap;
import org.rabbit.sql.utils.SqlUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.rabbit.sql.utils.SqlUtil.unWrapValue;

/**
 * SQL条件拼装器
 */
public class Condition implements ICondition {
    private final Map<String, Param> params = new HashMap<>();

    private final StringBuilder where = new StringBuilder();
    private int arg_index = 0;
    private List<String> orders;

    private Condition() {

    }

    /**
     * 创建一个新的条件拼接器
     *
     * @return 条件拼接器
     */
    public static Condition New() {
        return new Condition();
    }

    /**
     * where
     *
     * @param filter 过滤器
     * @return 条件拼接器
     */
    public Condition where(IFilter filter) {
        if (filter.getValue() != IFilter.IGNORE_VALUE) {
            Pair<String, String> sf = getSpecialField(filter.getField(), filter.getValue());
            where.append(" where ").append(filter.getField()).append(filter.getOperator()).append(sf.getItem2());
            params.put(sf.getItem1(), Param.IN(unWrapValue(filter.getValue())));
        } else {
            where.append(" where ").append(filter.getField()).append(filter.getOperator());
        }
        return this;
    }

    /**
     * 排序
     *
     * @param field 字段名
     * @return 条件拼接器
     */
    public Condition orderBy(String field) {
        return orderBy(field, Order.ASC);
    }

    /**
     * 排序
     *
     * @param field 字段名
     * @param order 排序枚举
     * @return 条件拼接器
     */
    public Condition orderBy(String field, Order order) {
        if (orders == null) {
            orders = new ArrayList<>();
        }
        orders.add(field + " " + order.getValue());
        return this;
    }

    /**
     * and
     *
     * @param filter 过滤器
     * @param more   更多过滤器
     * @return 条件拼接器
     */
    public Condition and(IFilter filter, IFilter... more) {
        return concatFilterBy(" and ", filter, more);
    }

    /**
     * or
     *
     * @param filter 过滤器
     * @param more   更多过滤器
     * @return 条件拼接器
     */
    public Condition or(IFilter filter, IFilter... more) {
        return concatFilterBy(" or ", filter, more);
    }

    /**
     * 通过某个字符串对多组条件进行连接
     *
     * @param s      连接字符串
     * @param filter 过滤器
     * @param more   更多过滤器
     * @return 条件拼接器
     */
    private Condition concatFilterBy(String s, IFilter filter, IFilter... more) {
        if (more.length == 0) {
            if (filter.getValue() != IFilter.IGNORE_VALUE) {
                Pair<String, String> sf = getSpecialField(filter.getField(), filter.getValue());
                where.append(s).append(filter.getField()).append(filter.getOperator()).append(sf.getItem2());
                params.put(sf.getItem1(), Param.IN(unWrapValue(filter.getValue())));
            } else {
                where.append(s).append(filter.getField()).append(filter.getOperator());
            }
            return this;
        }
        IFilter[] filters = new IFilter[1 + more.length];
        filters[0] = filter;
        System.arraycopy(more, 0, filters, 1, more.length);
        String childAndGroup = Stream.of(filters).map(f -> {
            if (f.getValue() != IFilter.IGNORE_VALUE) {
                Pair<String, String> sf = getSpecialField(f.getField(), f.getValue());
                String and = f.getField() + f.getOperator() + sf.getItem2();
                params.put(sf.getItem1(), Param.IN(unWrapValue(f.getValue())));
                return and;
            }
            return f.getField() + f.getOperator();
        }).collect(Collectors.joining(s));
        where.append(s).append("( ").append(childAndGroup).append(")");
        return this;
    }

    /**
     * 获取经过特殊字符和自动编号处理的字段名占位符
     *
     * @param field 字段名
     * @param value 字段值
     * @return (字段名, 字段名占位符)
     */
    private Pair<String, String> getSpecialField(String field, Object value) {
        String s = field + "_" + SqlUtil.SEP + arg_index++;
        if (value instanceof ValueWrap) {
            ValueWrap wrapV = (ValueWrap) value;
            return Pair.of(s, wrapV.getStart() + " :" + s + wrapV.getEnd());
        }
        return Pair.of(s, ":" + s);
    }

    @Override
    public Map<String, Param> getParams() {
        return params;
    }

    @Override
    public String getString() {
        if (orders == null) {
            return where.toString();
        }
        return where.toString() + " order by " + String.join(", ", orders);
    }
}
