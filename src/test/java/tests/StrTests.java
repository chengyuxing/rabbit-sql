package tests;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

public class StrTests {

    @Test
    public void testDtFmt() throws Exception {
        System.out.println(SqlUtil.quoteFormatValue(LocalTime.now()));
    }

    @Test
    public void test1() throws Exception {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("x", "pgsql/multi.xql");
        xqlFileManager.setDelimiter(null);
        xqlFileManager.setPipes(Args.of("len", "tests.Length"));
        xqlFileManager.init();
    }

    @Test
    public void keys() throws Exception {
        String sql = "with t(a, b) as (select * from (values (array [ 1,2,'3,4,5'], /*array [[6',7,8,9'],*/[10,11,12,13] ])) arr)\n" +
                "select a[3:5],\n" +
                "       a[:2],\n" +
                "       a[2:],\n" +
                "    /*a[:],\n" +
                "    a[4:4],\n" +
                "    b/*['1':2]*/[2],\n" +
                "    b[:2][2:],*/\n" +
                "       array_dims(a),      -- 数组区间\n" +
                "       array_dims(b),\n" +
                "       array_upper(a, 1),  -- 区间结尾\n" +
                "       array_lower(a, 1),  -- 区间开始\n" +
                "       array_length(b, 1), -- 长度\n" +
                "       cardinality(b)      --所有元素个数\n" +
                "from t;";

        System.out.println(SqlUtil.removeAnnotationBlock(sql));
        SqlUtil.getAnnotationBlock(sql).forEach(a -> {
            System.out.println(a);
            System.out.println("---");
        });
        System.out.println(SqlUtil.highlightSql(sql));
    }

    @Test
    public void xsz() throws Exception {
        System.out.println("\u02ac");
    }

    @Test
    public void gu() throws Exception {
        update("test.user",
                Args.<Object>of("id", 10)
                        .add("name", "chengyuxing")
                        .add("now", LocalDateTime.now())
                        .add("enable", true),
                "id=:id and enable = :enable");
    }

    @Test
    public void testUpdate() {
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            Map<String, Object> args = Args.create(
                    "id", 10,
                    "name", "chengyuxing",
                    "age", i,
                    "address", "昆明",
                    "work", "java"
            );
            String update = sqlGenerator.generateUpdate(
                    "test.user",
                    "id = :id and name = :name",
                    args,
                    Arrays.asList("name", "age", "work"),
                    false);
            list.add(update);
        }
        System.out.println(list.size());
        System.out.println(list.get(7810));
    }


    public static void update(String tableName, Map<String, Object> data, String where) {
        Pair<String, List<String>> cnd = new SqlGenerator(':').generateSql(where, data, true);
        Map<String, Object> updateData = new HashMap<>(data);
        for (String key : cnd.getItem2()) {
            updateData.remove(key);
        }
//        String update = new SqlGenerator(':').generateNamedParamUpdate(tableName, updateData);
//        String w = StringUtil.startsWithIgnoreCase(where.trim(), "where") ? where : "\nwhere " + where;
//        System.out.println(update + w);
//        System.out.println(data);
    }

    @Test
    public void tst35() throws IOException {
        String sql = SqlUtil.highlightSql("select 1,2,3, current_timestamp;");
        Files.write(Paths.get("/Users/chengyuxing/Downloads/bbb.txt"), sql.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void yesy36() {
        SqlGenerator generator = new SqlGenerator(':');
        String sql = "select * from test.user where name = ${:name}";
        String pair = generator.formatSql(sql, Args.create("name", "'; drop table test.user;--'"));
        System.out.println(pair);
    }

    @Test
    public void test37() {
        getClazz(null);
    }

    public static void getClazz(Object o) {
        System.out.println(o.getClass());
    }
}
