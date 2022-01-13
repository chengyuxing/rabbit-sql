package sql;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class DynamicTests {

    static XQLFileManager sqlFileManager = new XQLFileManager();

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        sqlFileManager.add("nest", "pgsql/deep_nest.sql");
        sqlFileManager.init();
        System.out.println("------------------");
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
}
