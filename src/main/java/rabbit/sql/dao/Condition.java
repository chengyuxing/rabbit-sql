package rabbit.sql.dao;

import rabbit.common.tuple.Pair;
import rabbit.sql.support.IFilter;
import rabbit.sql.support.IOrderBy;
import rabbit.sql.types.Order;
import rabbit.sql.types.Param;
import rabbit.sql.types.ValueWrap;
import rabbit.sql.utils.SqlUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static rabbit.sql.utils.SqlUtil.unWrapValue;

/**
 * SQL条件拼装器
 */
public class Condition implements IOrderBy {
    private final Map<String, Param> params = new HashMap<>();

    private String where = "";
    private final StringBuilder conditions = new StringBuilder();
    private IOrderBy orderBy;
    private int arg_index = 0;

    Condition() {

    }

    public static Condition where() {
        Condition condition = new Condition();
        if (condition.where.equals("")) {
            condition.where = " where ";
        }
        return condition;
    }

    /**
     * where
     *
     * @param filter 过滤器
     * @return 条件拼接器
     */
    public static Condition where(IFilter filter) {
        return where().concatFilterBy("", filter);
    }

    /**
     * 返回一个OrderBy实例
     *
     * @return orderly
     */
    public static IOrderBy orderBy() {
        return new OrderBy();
    }

    /**
     * 排序
     *
     * @param field 字段名
     * @param order 排序枚举
     * @return 条件拼接器
     */
    @Override
    public IOrderBy orderBy(String field, Order order) {
        if (orderBy == null) {
            orderBy = new OrderBy();
        }
        orderBy.orderBy(field, order);
        return this;
    }

    /**
     * 一段原生sql表达式
     *
     * @param sql sql
     * @return 条件拼接器
     */
    public Condition expression(String sql) {
        conditions.append(" ").append(sql).append(" ");
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
        if (where.equals(" where ")) {
            //  去除
        }
        if (more.length == 0) {
            if (filter.getValue() != IFilter.IGNORE_VALUE) {
                Pair<String, String> sf = getSpecialField(filter.getField(), filter.getValue());
                conditions.append(s).append(filter.getField()).append(filter.getOperator()).append(sf.getItem2());
                params.put(sf.getItem1(), Param.IN(unWrapValue(filter.getValue())));
            } else {
                conditions.append(s).append(filter.getField()).append(filter.getOperator());
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
        conditions.append(s).append("( ").append(childAndGroup).append(")");
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
    public String getSql() {
        String cnds = conditions.toString();
        if (orderBy != null) {
            if (cnds.startsWith(" and ")) {
                cnds = cnds.substring(5);
            }
            return "\n" + where + cnds + orderBy.getSql();
        }
        return "\n" + where + cnds;
    }
}
