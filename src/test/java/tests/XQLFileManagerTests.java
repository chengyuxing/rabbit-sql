package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.yaml.JoinConstructor;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.Map;

public class XQLFileManagerTests {
    @Test
    public void test() {
        Yaml yaml = new Yaml(new JoinConstructor());
//        yaml.addImplicitResolver(new Tag("!!merge"), Pattern.compile("!!merge"), "[");
        Map<String, Object> res = yaml.loadAs(new FileResource("xql-file-manager.yml").getInputStream(), Map.class);
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
        files.put("nest", "pgsql/nest.sql");
        XQLFileManager xqlFileManager = new XQLFileManager(files);
        xqlFileManager.setHighlightSql(true);
        xqlFileManager.init();
        System.out.println(xqlFileManager);
    }
}
