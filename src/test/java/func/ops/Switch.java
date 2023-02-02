package func.ops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Switch {
    private final String key;
    private final List<Case> cases = new ArrayList<>();

    public Switch(String key, Case case_, Case... more) {
        this.key = key;
        this.cases.add(case_);
        this.cases.addAll(Arrays.asList(more));
    }

    public static Case case_(String value, String sql) {
        return new Case(value, sql);
    }

    public static Case default_(String sql) {
        return new Case("", sql) {
            @Override
            public String toString() {
                return "--#default\n\t\t" + sql + "\n\t--#break";
            }
        };
    }

    @Override
    public String toString() {
        return "--#switch :" + key + "\n\t" +
                cases.stream().map(Case::toString).collect(Collectors.joining("\n\t")) +
                "\n--#end";
    }
}
