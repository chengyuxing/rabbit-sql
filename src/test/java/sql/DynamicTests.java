package sql;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicTests {

    static XQLFileManager sqlFileManager = new XQLFileManager();

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        sqlFileManager.add("nest", "pgsql/deep_nest.sql");
        sqlFileManager.add("for", "pgsql/for.sql");
        sqlFileManager.setPipeInstances(Args.of("is_id_card", new IsIdCardPipe()));
//        sqlFileManager.setPipes(Args.of("is_id_card","sql.IsIdCardPipe"));
        sqlFileManager.init();
        System.out.println("------------------");
    }

    @Test
    public void test() throws Exception {
        List<Map<String, Object>> users = Arrays.asList(Args.of("name", "cyx"), Args.of("name", "jack"), Args.of("name", "lisa"));
        List<String> names = Arrays.asList("jackson", "mike", "book", "Bob");
        Args<Object> args = Args.create("users", users, "names", names, "id", 10);
        System.out.println(sqlFileManager.get("for.q", args));
        System.out.println(sqlFileManager.get("for.q2", args));
    }

    @Test
    public void test3() throws Exception {
        System.out.println(sqlFileManager.get("for.pipe",Args.create("idCard","5301111993")));
    }

    @Test
    public void test4() throws Exception{
        System.out.println(sqlFileManager.get("for.switch", Args.of("name", "cyx")));
    }

    @Test
    public void test2() throws Exception {
        System.out.println(sqlFileManager.get("nest.choose", Args.create("id", null, "name", "cyx")));
    }

    @Test
    public void test1() throws Exception {
        Args<Object> args = Args.create(
                "a", 9,
                "a1", 98,
                "a2", 1,
                "b", 8,
                "c", 7,
                "c1", null,
                "c2", 9,
                "cc1", 1,
                "cc2", 1,
                "e", null,
                "f", 23,
                "g", 34,
                "x", 87,
                "xx", null,
                "y", 98,
                "yy", null,
                "ff", null,
                "name", "78"
        );
        String sql = sqlFileManager.get("nest.region", args);
        System.out.println(sql);
    }

    @Test
    public void forTest() throws Exception {
        String forExp = "for item of :items separator ','";
        Pattern p = Pattern.compile("for\\s+(?<var>\\w+)\\s+of\\s+:\\w+\\s+");

        Matcher m = p.matcher(forExp);
        if (m.find()) {
            System.out.println(m.group("var"));
        }

        String[] arr = new String[]{"a", "b", "c", "d"};

    }
}
