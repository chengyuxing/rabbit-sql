package func;

import rabbit.common.tuple.Pair;
import rabbit.sql.utils.SqlUtil;

import java.util.HashMap;
import java.util.Map;

public class FCondition<T> {
    private final Map<String, Object> args = new HashMap<>();
    private final StringBuilder conditions = new StringBuilder();
    private int arg_index = 0;

    FCondition() {

    }

    /**
     * where
     *
     * @param filter 过滤器
     * @return 条件拼接器
     */
    public static <T> FCondition<T> where(FFilter<T> filter) {
        FCondition<T> cnd = new FCondition<>();
        return cnd.concatFilterBy("", filter);
    }

    /**
     * 一段原生sql表达式
     *
     * @param sql sql
     * @return 条件拼接器
     */
    public FCondition<T> expression(String sql) {
        conditions.append(sql).append(" ");
        return this;
    }

    /**
     * and
     *
     * @param filter 过滤器
     * @return 条件拼接器
     */
    public FCondition<T> and(FFilter<T> filter) {
        return concatFilterBy("and ", filter);
    }

    /**
     * or
     *
     * @param filter 过滤器
     * @return 条件拼接器
     */
    public FCondition<T> or(FFilter<T> filter) {
        return concatFilterBy("or ", filter);
    }

    /**
     * 通过某个字符串对多组条件进行连接
     *
     * @param s      连接字符串
     * @param filter 过滤器
     * @return 条件拼接器
     */
    private FCondition<T> concatFilterBy(String s, FFilter<T> filter) {
        Pair<String, String> sf = getSpecialField(filter.getField());
        conditions.append(s).append(filter.getField()).append(filter.getOperator()).append(sf.getItem2()).append(" ");
        args.put(sf.getItem1(), filter.getValue());
        return this;
    }

    /**
     * 获取经过特殊字符和自动编号处理的字段名占位符
     *
     * @param field 字段名
     * @return (字段名, 字段名占位符)
     */
    private Pair<String, String> getSpecialField(String field) {
        String s = field + SqlUtil.SEP + arg_index++;
        return Pair.of(s, ":" + s);
    }

    /**
     * 获取参数
     *
     * @return 参数
     */
    public Map<String, Object> getArgs() {
        return args;
    }

    /**
     * 获取拼接的where子句
     *
     * @return where子句
     */
    public String getSql() {
        String cnds = conditions.toString();
        if (cnds.startsWith("and ")) {
            cnds = cnds.substring(4);
        } else if (cnds.startsWith("or ")) {
            cnds = cnds.substring(3);
        }
        return "\n where " + cnds;
    }
}
