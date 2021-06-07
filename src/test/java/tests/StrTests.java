package tests;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.script.Comparators;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Keywords;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class StrTests {
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

//        Pair<String, List<String>> pair = SqlUtil.removeAnnotationBlock(sql);
//        System.out.println(pair.getItem1());
//        System.out.println(pair.getItem2());;
//
        System.out.println(SqlUtil.highlightSql(sql));
    }

    @Test
    public void xsz() throws Exception {
        System.out.println("\u02ac");
    }
}
