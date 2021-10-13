package entity;

import com.github.chengyuxing.sql.support.IOutParam;

public class PValue {
    private final Object value;
    private final PMode mode;
    private final IOutParam type;

    public PValue(Object value, PMode mode,IOutParam type) {
        this.value = value;
        this.mode = mode;
        this.type = type;
    }

    public IOutParam getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public PMode getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return "PValue{" +
                "value=" + value +
                ", mode=" + mode +
                ", type=" + type +
                '}';
    }
}
