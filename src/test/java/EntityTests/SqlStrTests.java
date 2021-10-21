package EntityTests;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.Map;

public class SqlStrTests {
    @Test
    public void test1() throws Exception {
        Map<String, Object> args = Args.<Object>of("id", 5)
                .add("name", "cyx")
                .add("email", "chengyuxingo@gmail.com")
                .add("${and}", "and name = :name");
        System.out.println(SqlUtil.generateUpdate("test.user", args, "id = :id ${and}"));
    }
}
