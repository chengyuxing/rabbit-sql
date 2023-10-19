package baki;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.Jackson;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import org.junit.Test;

import java.util.Collections;

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

    @Test
    public void test2() {
        PageHelper pageHelper = new PGPageHelper();
        pageHelper.init(1, 15, 45);
        PagedResource<DataRow> pagedResource = PagedResource.of(pageHelper, Collections.singletonList(DataRow.of("a", 1, "b", 2)));
        String j = Jackson.toJson(pagedResource);
        System.out.println(j);
    }
}
