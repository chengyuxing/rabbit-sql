package com.github.chengyuxing.sql.types;

import com.github.chengyuxing.sql.support.IOutParam;

/**
 * Store procedure/function parameter object.
 */
public final class Param {
    private Object value;
    private IOutParam type;
    private ParamMode paramMode;

    /**
     * Constructed an IN parameter.
     *
     * @param value value
     * @return Param
     */
    public static Param IN(Object value) {
        Param param = new Param();
        param.value = value;
        param.paramMode = ParamMode.IN;
        return param;
    }

    /**
     * Constructed an OUT parameter.
     *
     * @param type OUT parameter type
     * @return Param
     */
    public static Param OUT(IOutParam type) {
        Param param = new Param();
        param.type = type;
        param.paramMode = ParamMode.OUT;
        return param;
    }

    /**
     * Constructed an IN_OUT parameter.
     *
     * @param value IN value
     * @param type  OUT parameter type
     * @return Param
     */
    public static Param IN_OUT(Object value, IOutParam type) {
        Param param = new Param();
        param.paramMode = ParamMode.IN_OUT;
        param.type = type;
        param.value = value;
        return param;
    }

    public Object getValue() {
        return value;
    }

    public ParamMode getParamMode() {
        return paramMode;
    }

    public IOutParam getType() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (value != null)
            sb.append("value=")
                    .append(value)
                    .append(", ");
        if (type != null)
            sb.append("type=")
                    .append(type)
                    .append(", ");
        sb.append("mode=")
                .append(paramMode)
                .append("}");
        return sb.toString();
    }
}
