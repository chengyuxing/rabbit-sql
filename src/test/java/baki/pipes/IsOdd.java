package baki.pipes;


import com.github.chengyuxing.common.script.expression.IPipe;

public class IsOdd implements IPipe<Boolean> {
    @Override
    public Boolean transform(Object value) {
        if (value instanceof Integer) {
            int num = (Integer) value;
            return (num & 1) != 0;
        }
        return false;
    }
}
