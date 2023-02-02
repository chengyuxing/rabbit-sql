package func;

import func.ops.*;

import java.util.ArrayList;
import java.util.List;

public class DynamicSqlBuilder {

    List<String> ifs = new ArrayList<>();

    public DynamicSqlBuilder if_(String expression, String sql) {
        ifs.add(new If(expression, sql).toString());
        return this;
    }

    public DynamicSqlBuilder if_(String expression, If if_) {
        ifs.add(new If(expression, if_).toString());
        return this;
    }

    public DynamicSqlBuilder switch_(String key, Case case_, Case... more) {
        ifs.add(new Switch(key, case_, more).toString());
        return this;
    }

    public DynamicSqlBuilder choose(When when, When... more) {
        ifs.add(new Choose(when, more).toString());
        return this;
    }

    public DynamicSqlBuilder for_(Object body, String delimiter, Object filter) {
        return this;
    }

    public String build() {
        return String.join("\n", ifs);
    }
}
