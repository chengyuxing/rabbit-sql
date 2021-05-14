package strtests;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.utils.SqlUtil;
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

    @Test
    public void ExcludePlaceholder() throws Exception {
        String sql = "select '${tableName}',${age} age from test.${tableName} where id = 10";
        Args<Object> args = Args.<Object>of("${tableName}", "user").add("${age}", 28);
        System.out.println(SqlUtil.resolveSqlPart(sql, args));
    }
}
