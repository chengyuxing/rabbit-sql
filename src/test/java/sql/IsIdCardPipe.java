package sql;


import com.github.chengyuxing.common.script.expression.IPipe;

public class IsIdCardPipe implements IPipe<Boolean> {
    @Override
    public Boolean transform(Object value) {
        if (value == null) {
            return false;
        }
        return value.toString().matches("\\d{17}[\\dxX]");
    }
}
