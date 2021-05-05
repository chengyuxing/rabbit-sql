package strtests;

import org.junit.Test;

import static com.github.chengyuxing.sql.utils.SqlUtil.removeAnnotationBlock;

public class StrTests {

    public static String sql = "/*我的注释*/\n" +
            "select count(*)\n" +
            "from test.student t\n" +
            "WHERE\n" +
            "--#if :age !=null\n" +
            "    t.age > 21\n" +
            "--#fi\n" +
            "--#if :name != null\n" +
            "  and t.name ~ :name\n" +
            "--#fi\n" +
            "--#if :age <> blank && :age < 90\n" +
            "and age < 90\n" +
            "--#fi\n" +
            ";";

    @Test
    public void sqlTest() throws Exception {
        System.out.println(removeAnnotationBlock(sql));
    }

}
