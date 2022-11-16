package tests;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StrTests {

    @Test
    public void test1() throws Exception{
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("x","pgsql/multi.xql");
        xqlFileManager.setDelimiter(null);
        xqlFileManager.setHighlightSql(true);
        xqlFileManager.init();
        xqlFileManager.look();
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


    public static void update(String tableName, Map<String, Object> data, String where) {
        Pair<String, List<String>> cnd = new SqlTranslator(':').generateSql(where, data, true);
        Map<String, Object> updateData = new HashMap<>(data);
        for (String key : cnd.getItem2()) {
            updateData.remove(key);
        }
        String update = new SqlTranslator(':').generateNamedParamUpdate(tableName, updateData);
        String w = StringUtil.startsWithIgnoreCase(where.trim(), "where") ? where : "\nwhere " + where;
        System.out.println(update + w);
        System.out.println(data);
    }
}
