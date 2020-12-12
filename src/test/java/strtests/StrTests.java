package strtests;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static rabbit.sql.utils.SqlUtil.removeAnnotationBlock;

public class StrTests {

    public static String sql = "with t(a, b) as (select * from (values (array [ 1,2,3,4,5], /*array [[6,7,8,9],*/[10,11,12,13]])) arr)\n" +
            "select a[3:5],\n" +
            "       a[:2],\n" +
            "       a[2:],\n" +
            "       /*a[:],\n" +
            "       a[4:4],\n" +
            "       b/*[1:2]*/[2],\n" +
            "       b[:2][2:],*/\n" +
            "       '/*array_dims(a)*/' as arr,      -- 数组区间\n" +
            "       array_dims(b),\n" +
            "       array_upper(a, 1),  -- 区间结尾\n" +
            "       array_lower(a, 1),  -- 区间开始\n" +
            "       array_length(b, 1), -- 长度\n" +
            "       cardinality(b)      --所有元素个数\n" +
            "from t;";

    @Test
    public void sqlTest() throws Exception {
        System.out.println(removeAnnotationBlock(sql));
    }

}
