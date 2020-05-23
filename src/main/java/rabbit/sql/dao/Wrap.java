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
     * 前后包裹
     * <p>e.g PostgreSQL: select age(date '1993-5-10 12:34:55'::timestamp)</p>
     * <p>e.g wrap("date", "1993-5-10 12:34:55", "::timestamp")</p>
     *
     * @param start 前缀
     * @param value 值
     * @param end   后缀
     * @return 值包装器
     */
    public static Wrap wrap(String start, Object value, String end) {
        return new Wrap(start, value, end);
    }

    /**
     * 前包裹
     * <p>e.g PostgreSQL: select age(timestamp '1993-5-10')</p>
     * <p>e.g wrap("timestamp","1993510")</p>
     *
     * @param start 前缀
     * @param value 值
     * @return 值包装器
     */
    public static Wrap wrapStart(String start, Object value) {
        return new Wrap(start, value, "");
    }

    /**
     * 后包裹
     * <p>e.g PostgreSQL: select age('1993-5-10'::timestamp)</p>
     * <p>e.g wrap("1993510", "::timestamp")</p>
     *
     * @param value 值
     * @param end   后缀
     * @return 值包装器
     */
    public static Wrap wrapEnd(Object value, String end) {
        return new Wrap("", value, end);
    }

    public String getEnd() {
        return end;
    }

    public String getStart() {
        return start;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
