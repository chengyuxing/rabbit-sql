package func.ops;

public class If {
    protected String expression;
    protected String sql;
    protected If child;
    protected int deep = 0;

    public If(String expression, String sql) {
        this.expression = expression;
        this.sql = sql;
    }

    public If(String expression, If if_) {
        this.expression = expression;
        this.child = if_;
    }

    public static If of(String expression, String sql) {
        return new If(expression, sql);
    }

    public static If of(String expression, If if_) {
        return new If(expression, if_);
    }

    protected String repeatTab(int count) {
        if (count <= 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        do {
            sb.append("\t");
            count--;
        } while (count > 0);
        return sb.toString();
    }

    @Override
    public String toString() {
        String sqlBlock = "\t" + sql;
        if (child != null) {
            child.deep = deep + 1;
            sqlBlock = child.toString();
        }
        int i = deep, j = deep, k = deep;
        if (child == null && i > 1) {
            i -= 1;
        }
        return repeatTab(i) + "--#if " + expression +
                "\n" + repeatTab(j) + sqlBlock + "\n" +
                repeatTab(k) + "--#fi";
    }
}
