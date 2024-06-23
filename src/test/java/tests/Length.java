package tests;


import com.github.chengyuxing.common.script.expression.IPipe;

public class Length implements IPipe<Integer> {
    @Override
    public Integer transform(Object value) {
        return value.toString().length();
    }
}
