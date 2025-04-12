package sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



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
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        System.out.println(sqlGenerator.generateSql(sql, DataRow.of()));
    }

    @Test
    public void testss() throws Exception {
        String sql = "insert into user (x, xm ,xb) values (:x, :xb, :xm, :x)";

        Pattern PARAM_PATTERN = Pattern.compile("(^:|[^:]:)(?<name>[a-zA-Z_][\\w_]*)", Pattern.MULTILINE);
        Matcher m = PARAM_PATTERN.matcher(sql);
        while (m.find()) {
//            System.out.println(m.group("name"));
//            sql = StringUtil.replaceFirstIgnoreCase(sql, ":" + m.group("name"), "?");
        }

        System.out.println(sql);


//        System.out.println(StringUtil.replaceIgnoreCase(sql,":X","@"));
//        System.out.println(StringUtil.replaceIgnoreCaseFirst(sql,":X","@"));

//        System.out.println(StringUtil.replaceIgnoreCase(sql, "?x", "?"));
    }

    @Test
    public void asd() throws Exception {
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
        DataRow args = DataRow.<Object>of("ids", Arrays.asList("I'm Ok!", "b", "c"))
                .add("fields", "id, name, age")
                .add("id", Math.random())
                .add("ids", Arrays.asList("a", "b", "c"))
                .add("date", "2020-12-23 ${:time}")
                .add("time", "11:23:44")
                .add("cnd", "id in (${:ids},${fields}) and id = :id or ${date} '${mn}' and ${");

//        mergeMap(args);

//        System.out.println(new SqlGenerator(':').formatSql(sql, args));
//        String[] sqls = new String[1000];
//        for (int i = 0; i < 1000; i++) {
//            String sqlx = new SqlGenerator(':').generateSql(sql, args, false).getItem1();
//            sqls[i] = sqlx;
//        }
//        System.out.println(sqls.length);
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        System.out.println(SqlUtil.formatSql(sql, args));

        System.out.println("-----");
        String first = SqlUtil.formatSql(sql, args);
        System.out.println(first);
//        String second = sqlGenerator.resolveSqlStrTemplateRec2(first, args);
//        System.out.println(second);
//        String third = sqlGenerator.resolveSqlStrTemplateRec2(second, args);
//        System.out.println(third);
//        String fourth = sqlGenerator.resolveSqlStrTemplateRec2(third, args);
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
