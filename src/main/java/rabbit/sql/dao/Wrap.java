package rabbit.sql.dao;

/**
 * 值包装器
 */
public class Wrap {
    private final Object value;
    private final String start;
    private final String end;

    Wrap(String start, Object value, String end) {
        this.value = value;
        this.start = start;
        this.end = end;
    }

    /**
     * 前后包裹<br>
     * e.g. PostgreSQL:
     * <blockquote>
     *     <pre>select age(date '1993-5-10 12:34:55'::timestamp)</pre>
     * </blockquote>
     * 使用：
     * <blockquote>
     *     <pre>wrap("date", "1993-5-10 12:34:55", "::timestamp")</pre>
     * </blockquote>
     * @param start 前缀
     * @param value 值
     * @param end   后缀
     * @return 值包装器
     */
    public static Wrap wrap(String start, Object value, String end) {
        return new Wrap(start, value, end);
    }

    /**
     * 前包裹<br>
     * e.g. PostgreSQL:
     * <blockquote>
     *     <pre>select age(timestamp '1993-5-10')</pre>
     * </blockquote>
     * 使用：
     * <blockquote>
     *     <pre>wrap("timestamp","1993510")</pre>
     * </blockquote>
     *
     * @param start 前缀
     * @param value 值
     * @return 值包装器
     */
    public static Wrap wrapStart(String start, Object value) {
        return new Wrap(start, value, "");
    }

    /**
     * 后包裹<br>
     * e.g. PostgreSQL:
     * <blockquote>
     *     <pre>select age('1993-5-10'::timestamp)</pre>
     * </blockquote>
     * 使用：
     * <blockquote>
     *     <pre>wrap("1993510", "::timestamp")</pre>
     * </blockquote>
     *
     * @param value 值
     * @param end   后缀
     * @return 值包装器
     */
    public static Wrap wrapEnd(Object value, String end) {
        return new Wrap("", value, end);
    }

    /**
     * 获取后缀
     * @return 后缀
     */
    public String getEnd() {
        return end;
    }

    /**
     * 获取前缀
     * @return 前缀
     */
    public String getStart() {
        return start;
    }

    /**
     * 获取值
     * @return 值
     */
    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
