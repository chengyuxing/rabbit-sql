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
        System.out.println(xqlFileManager.allFiles());
    }

    @Test
    public void test4() {
        Map<String, String> files = new HashMap<>();
        files.put("data", "pgsql/data.sql");
//        files.put("nest", "pgsql/nest.sql");
        XQLFileManager xqlFileManager = new XQLFileManager(files);
        xqlFileManager.add("pgsql/nest.sql");
        xqlFileManager.setHighlightSql(true);
        xqlFileManager.init();

        xqlFileManager.remove("nest");
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
}
