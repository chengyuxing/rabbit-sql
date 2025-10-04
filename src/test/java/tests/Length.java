package tests;


import com.github.chengyuxing.common.script.pipe.IPipe;

public class Length implements IPipe<Integer> {
    @Override
    public Integer transform(Object value, Object... params) {
        return value.toString().length();
    }
}
