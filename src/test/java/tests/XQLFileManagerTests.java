package tests;

import baki.pipes.IsOdd;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.yaml.FeaturedConstructor;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class XQLFileManagerTests {
    @Test
    public void test() {
        Yaml yaml = new Yaml(new FeaturedConstructor());
//        yaml.addImplicitResolver(new Tag("!!merge"), Pattern.compile("!!merge"), "[");
        Map<String, Object> res = yaml.loadAs(new FileResource("xql-file-manager.old.yml").getInputStream(), Map.class);
        System.out.println(res);
    }

    @Test
    public void test1() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        System.out.println(xqlFileManager);
    }

    @Test
    public void test4() throws IOException, URISyntaxException {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("a", "pgsql/deep_nest.sql");
        xqlFileManager.init();

        System.out.println("-----");
        System.out.println(xqlFileManager.get("a.region", Args.of(
                "id", 15,
                "name", "cyx",
                "a", "bbb",
                "a2", "ccc",
                "x", "fff",
                "b", "ddd",
                "c1", "ttt",
                "c", "aaa",
                "e", null,
                "f", "qqq",
                "ff", null,
                "list", Arrays.asList(1, 2, 3, 4, 5, 6)
        )));
    }

    @Test
    public void test11() throws URISyntaxException, IOException {
        System.out.println(new FileResource("b.json").getLastModified());
        System.out.println(new FileResource("template.xql").getLastModified());
        System.out.println("---");
        System.out.println(new FileResource("file:/Users/chengyuxing/IdeaProjects/rabbit-sql/src/test/resources/pgsql/data.sql").getLastModified());
        System.out.println(new FileResource("file:/Users/chengyuxing/IdeaProjects/rabbit-sql/src/test/resources/pgsql/nest.sql").getLastModified());
    }

    @Test
    public void test12() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", "b");
        map.put("b", "a");
        map.put("name", "cyx");
        map.put("cyx", "name");
        map.put("bbc", "name");

        map.remove(map.remove("bbc"));
        System.out.println(map);
    }

    @Test
    public void test78() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("new", "pgsql/new_for.sql");
        xqlFileManager.setPipeInstances(Args.of("isOdd", new IsOdd()));
        xqlFileManager.init();

        Pair<String, Map<String, Object>> result1 = xqlFileManager.get("new.query",
                DataRow.of("ids", Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        );

        System.out.println(result1);

        Pair<String, Map<String, Object>> result2 = xqlFileManager.get("new.update",
                DataRow.of(
                        "id", 12,
                        "data", DataRow.of("id", 1, "name", "cyx", "age", 30, "address", "kunming")
                ));

        System.out.println(result2);
    }

    @Test
    public void hello() {
        System.out.println(Math.random() * 500);
    }
}
