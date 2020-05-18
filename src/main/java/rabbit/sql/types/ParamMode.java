package rabbit.sql.types;

/**
 * 参数类型
 */
public enum ParamMode {
    /**
     * 入参
     */
    IN,
    /**
     * 出参
     */
    OUT,
    /**
     * 出入参
     */
    IN_OUT,
    /**
     * SQL模版代码片段
     */
    TEMPLATE
}
