package func.ops;

public class When extends If {
    public When(String expression, String sql) {
        super(expression, sql);
    }

    @Override
    public String toString() {
        return "--#when " + expression + "\n\t\t" + sql + "\n\t--#break";
    }
}
