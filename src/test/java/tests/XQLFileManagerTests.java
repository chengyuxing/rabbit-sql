package tests;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.yaml.JoinConstructor;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class XQLFileManagerTests {
    @Test
    public void test() {
        Yaml yaml = new Yaml(new JoinConstructor());
//        yaml.addImplicitResolver(new Tag("!!merge"), Pattern.compile("!!merge"), "[");
        Map<String, Object> res = yaml.loadAs(new FileResource("xql-file-manager.old.yml").getInputStream(), Map.class);
        System.out.println(res);
    }

    @Test
    public void test1() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        System.out.println(xqlFileManager);
        System.out.println(xqlFileManager.isHighlightSql());
    }

    @Test
    public void test4() throws IOException, URISyntaxException {
        XQLFileManager xqlFileManager = new XQLFileManager(new HashMap<>());
        xqlFileManager.add("a", "pgsql/nest.sql");
        xqlFileManager.add("b", "pgsql/nest.sql");
        xqlFileManager.init();

        System.out.println("-----");
//        boolean refreshed = xqlFileManager.getResource("a").refresh();
//        System.out.println(refreshed);
        xqlFileManager.removeByFilename("pgsql/nest.sql");
        System.out.println(xqlFileManager);
    }

    @Test
    public void test11() throws URISyntaxException, IOException {
        System.out.println(new FileResource("b.json").getLastModified());
        System.out.println(new FileResource("data.xql.template").getLastModified());
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
}
