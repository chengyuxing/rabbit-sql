package sql;

import com.github.chengyuxing.common.script.impl.CExpression;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.searchIndexUntilNotBlank;


public class ControlTest {

    @Test
    public void numberTest() throws Exception {
        double b = Double.valueOf("1584939393939");
        System.out.println(b);
    }

    @Test
    public void testxsz() throws Exception {
        String sql = "insert into user (x, xm ,xb) values (:xx,:x, :xm, :xb)";
        System.out.println(sql.length());
        SqlTranslator sqlTranslator = new SqlTranslator(':');
        System.out.println(sqlTranslator.generateSql(sql, Args.create(), true));
    }

    @Test
    public void testss() throws Exception {
        String sql = "insert into user (x, xm ,xb) values (:x, :xb, :xm, :x)";

        Pattern PARAM_PATTERN = Pattern.compile("(^:|[^:]:)(?<name>[a-zA-Z_][\\w_]*)", Pattern.MULTILINE);
        Matcher m = PARAM_PATTERN.matcher(sql);
        while (m.find()) {
//            System.out.println(m.group("name"));
            sql = StringUtil.replaceFirstIgnoreCase(sql, ":" + m.group("name"), "?");
        }

        System.out.println(sql);


//        System.out.println(StringUtil.replaceIgnoreCase(sql,":X","@"));
//        System.out.println(StringUtil.replaceIgnoreCaseFirst(sql,":X","@"));

//        System.out.println(StringUtil.replaceIgnoreCase(sql, "?x", "?"));
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
                skip = expression.calc(args, true);
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

        System.out.println(expression.calc(args, true));

    }

    @Test
    public void regex() throws Exception {
        Map<String, Object> args = new HashMap<>();
        args.put("enable", true);
        args.put("types", "a1b2c3");

        System.out.println(args.get("a"));

        CExpression expression = CExpression.of(":enable = false && :types @ \"\\w+\"");

        System.out.println(expression.calc(args, true));
    }

    @Test
    public void number() throws Exception {
        Pattern p = Pattern.compile("-?([0-9]|(0\\.\\d+)|([1-9]+\\.?\\d+))");
        System.out.println("-99.0".matches(p.pattern()));
    }

    @Test
    public void sqlPart() throws Exception {
        String sql = "select ${ fields } from test.user where ${cnd}";
        System.out.println(sql.length());
        Args<Object> args = Args.<Object>of("ids", Arrays.asList("I'm Ok!", "b", "c"))
                .add("fields", "id, name, age")
                .add("id", Math.random())
                .add("ids", Arrays.asList("a", "b", "c"))
                .add("date", "2020-12-23 ${:time}")
                .add("time", "11:23:44")
                .add("cnd", "id in (${:ids},${fields}) and id = :id or ${date} '${mn}' and ${");

//        mergeMap(args);

//        System.out.println(new SqlTranslator(':').formatSql(sql, args));
//        String[] sqls = new String[1000];
//        for (int i = 0; i < 1000; i++) {
//            String sqlx = new SqlTranslator(':').generateSql(sql, args, false).getItem1();
//            sqls[i] = sqlx;
//        }
//        System.out.println(sqls.length);
        SqlTranslator sqlTranslator = new SqlTranslator(':');
        System.out.println(sqlTranslator.formatSql(sql, args));

        System.out.println("-----");
        String first = sqlTranslator.formatSql(sql, args);
        System.out.println(first);
//        String second = sqlTranslator.resolveSqlStrTemplateRec2(first, args);
//        System.out.println(second);
//        String third = sqlTranslator.resolveSqlStrTemplateRec2(second, args);
//        System.out.println(third);
//        String fourth = sqlTranslator.resolveSqlStrTemplateRec2(third, args);
//        System.out.println(fourth);

    }

    public static void mergeMap(Map<String, Object> args) {
        Map<String, Object> deepMergedArgs = new HashMap<>(args);
        for (Map.Entry<String, ?> entry : deepMergedArgs.entrySet()) {
            String key = entry.getKey();
            String k1 = "${" + key + "}";
            String k2 = "${:" + key + "}";
            if (entry.getValue() instanceof String) {
                for (String x : deepMergedArgs.keySet()) {
                    String sql = deepMergedArgs.get(x).toString();
                    if (sql.contains(k1) || sql.contains(k2)) {
                        String sqlPart = deepMergedArgs.get(key).toString();
                        if (sql.contains(k1)) {
                            sql = sql.replace(k1, sqlPart);
                        } else {
                            sql = sql.replace(k2, sqlPart);
                        }
                        deepMergedArgs.put(x, sql);
                    }
                }
            }
        }
        deepMergedArgs.forEach((k, v) -> System.out.println(k + " -> " + v));
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

    @Test
    public void testMap() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("a", 1);
        data.put("b", null);
        data.put("name", "cyx");

        Map<String, ?> newMap = new HashMap<>(data);
        for (Map.Entry<String, ?> e : newMap.entrySet()) {
            if (e.getValue() == null) {
                newMap.remove(e.getKey());
            }
        }

        System.out.println(newMap);
        System.out.println(data);
        Integer a = 100000;
        System.out.println((int) a);
    }
}
