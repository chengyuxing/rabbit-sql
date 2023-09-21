package baki;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import org.junit.Test;

public class NonBakiTests {
    @Test
    public void test1() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.setDatabaseId("oracle");
        xqlFileManager.add("pgsql/other.sql");
        xqlFileManager.init();
        System.out.println("---");
        System.out.println(xqlFileManager.get("other.ooooooooo", Args.of()));
        System.out.println("---");
        System.out.println(xqlFileManager.get("other.other", Args.of("id", 1, "name", "null")));
    }
}
