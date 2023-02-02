package func.ops;

public class Case extends If {
    public Case(String expression, String sql) {
        super(expression, sql);
    }

    @Override
    public String toString() {
        return "--#case " + expression + "\n\t\t" + sql + "\n\t--#break";
    }
}
