package sql;

import com.github.chengyuxing.common.script.impl.CExpression;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.searchIndexUntilNotBlank;


public class ControlTest {

    @Test
    public void numberTest() throws Exception {
        double b = Double.valueOf("1584939393939");
        System.out.println(b);
    }

    @Test
    public void sqlResolve() throws Exception {
        String sql = "select *\n" +
                "from test.student t\n" +
                "where t.age > 21\n" +
                "  --#if :name == null\n" +
                "  and t.id < 19\n" +
                "  --#fi\n" +
                "  --#if :age < 50\n" +
                "  and age < 90\n" +
                "  --#fi\n" +
                "  and t.id > 2";

        Map<String, Object> args = new HashMap<>();
        args.put("name", "jack");
        args.put("age", "201");

        String[] sqls = sql.split("\n");
        StringBuilder sb = new StringBuilder();
        boolean skip = true;
        for (String line : sqls) {
            String trimLine = line.trim();
            if (trimLine.startsWith("--#if")) {
                String filter = trimLine.substring(6);
                CExpression expression = CExpression.of(filter);
                skip = expression.calc(args);
                continue;
            }
            if (trimLine.startsWith("--#fi")) {
                skip = true;
                continue;
            }
            if (skip) {
                sb.append(line).append("\n");
            }
        }

        System.out.println(sb.toString());
    }

    @Test
    public void asd() throws Exception {
    }

    @Test
    public void splitTest() throws Exception {
        int id = 15;
        int age = 21;
        String name = "  ";
        System.out.println(id >= 0 || name.equals("") && age <= 21);
        System.out.println(id >= 0 && name.equals("") || age <= 21);

        System.out.println((id > 0 || (age < 15 && name.equals(""))) || id == 15);

        Map<String, Object> args = new HashMap<>();
        args.put("id", 15);
        args.put("age", 21);
        args.put("name", " ");
        args.put("enable", true);
        args.put("types", "a1b2c3");

        System.out.println(args.get("a"));

        CExpression expression = CExpression.of(":enable = true && :name = blank && :age<=21");

        System.out.println(expression.calc(args));

    }

    @Test
    public void regex() throws Exception {
        Map<String, Object> args = new HashMap<>();
        args.put("enable", true);
        args.put("types", "a1b2c3");

        System.out.println(args.get("a"));

        CExpression expression = CExpression.of(":enable = false && :types @ \"\\w+\"");

        System.out.println(expression.calc(args));
    }

    @Test
    public void number() throws Exception {
        Pattern p = Pattern.compile("-?([0-9]|(0\\.\\d+)|([1-9]+\\.?\\d+))");
        System.out.println("-99.0".matches(p.pattern()));
    }

    @Test
    public void sqlPart() throws Exception {
        String sql = "select ${fields} from test.user where ${cnd}";
        System.out.println(sql.length());
        Args<Object> args = Args.<Object>of("ids", Arrays.asList("I'm Ok!", "b", "c"))
                .add("fields", "id, name, age")
                .add("id", 10)
                .add("cnd", "id in (${:ids},${fields}) and id = :id");

        System.out.println(SqlUtil.resolveSqlStrTemplate(sql, args));
    }

    @Test
    public void dynamicTest() throws Exception {
        XQLFileManager sqlFileManager = new XQLFileManager();
        sqlFileManager.add("dynamic", "pgsql/dynamic.sql");
        sqlFileManager.init();
        sqlFileManager.foreach((name, sql) -> {
            System.out.println(name + "-->" + sql);
        });
    }

    @Test
    public void xz() throws Exception {
        String str = "abcde";
        int length = str.length();
        int i = 0;
        while (i++ < 10) {
            System.out.println(i);
        }
    }

    @Test
    public void xxx() throws Exception {
        String a = "1   \t  \n      \t${abc}   \n";
        System.out.println(a.length());
        System.out.println(searchIndexUntilNotBlank(a, 5, false));
        System.out.println("abc".substring(0, 0));
    }
}
