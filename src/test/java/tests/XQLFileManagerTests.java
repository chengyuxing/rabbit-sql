package tests;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.XQLFileManagerConfig;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.Map;

public class XQLFileManagerTests {
    @Test
    public void test() {
        Yaml yaml = new Yaml(new JoinConstructor());
//        yaml.addImplicitResolver(new Tag("!!merge"), Pattern.compile("!!merge"), "[");
        XQLFileManager res = yaml.loadAs(new FileResource("xql-file-manager.yml").getInputStream(), XQLFileManager.class);
        System.out.println(res);
    }

    @Test
    public void test1() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        System.out.println(xqlFileManager);
        System.out.println(xqlFileManager.isHighlightSql());
    }
}
