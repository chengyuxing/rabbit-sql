package baki.pipes;


import com.github.chengyuxing.common.script.pipe.IPipe;

public class IsOdd implements IPipe<Boolean> {
    @Override
    public Boolean transform(Object value, Object... params) {
        if (value instanceof Integer) {
            int num = (Integer) value;
            return (num & 1) != 0;
        }
        return false;
    }
}
