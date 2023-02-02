package func.ops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Choose {
    private final List<When> whens = new ArrayList<>();

    public Choose(When when, When... more) {
        this.whens.add(when);
        this.whens.addAll(Arrays.asList(more));
    }

    public static When when(String expression, String sql) {
        return new When(expression, sql);
    }

    public static When default_(String sql) {
        return new When("", sql) {
            @Override
            public String toString() {
                return "--#default\n\t\t" + sql + "\n\t--#break";
            }
        };
    }

    @Override
    public String toString() {
        return "--#choose\n\t" +
                whens.stream().map(When::toString).collect(Collectors.joining("\n\t")) +
                "\n--#end";
    }
}
