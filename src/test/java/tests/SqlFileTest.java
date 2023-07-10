package tests;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nutz.dao.impl.NutDao;
import org.nutz.ioc.Ioc;
import org.nutz.ioc.impl.NutIoc;
import org.nutz.ioc.loader.json.JsonLoader;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public class SqlFileTest {

    private static Ioc ioc;

    //    @BeforeClass
    public static void init() {
        ioc = new NutIoc(new JsonLoader("ioc.js"));
    }

    @Test
    public void dynamicSqlFileManagerTest() throws Exception {
        XQLFileManager sqlFileManager = new XQLFileManager();
        sqlFileManager.add("data", "pgsql/data.sql");
        sqlFileManager.init();
        System.out.println("-------");
        System.out.println("-------");
        System.out.println("-------");
        System.out.println(sqlFileManager.get("data.logical", Args.<Object>of("name", "cyx").add("age", 101)));
        sqlFileManager.foreach((k, r) -> {
            System.out.println(k + "----->" + r);
        });
        System.out.println(sqlFileManager.size());
        System.out.println(sqlFileManager.names());
        System.out.println(sqlFileManager.contains("data.${order}"));
        System.out.println(sqlFileManager.contains("data.great.insert"));
    }

    @Test
    public void argsTest() throws Exception {
        System.out.println(Args.create("id", 1, "name", "cyx", "dt", LocalDateTime.now()));
    }

    @Test
    public void sqlf() throws Exception {
        XQLFileManager sqlFileManager = new XQLFileManager();
        sqlFileManager.add("rabbit", "file:/Users/chengyuxing/Downloads/local.sql");
        sqlFileManager.init();
    }

    public static void main(String[] args) {
        XQLFileManager sqlFileManager = new XQLFileManager();
        sqlFileManager.add("rabbit", "file:/Users/chengyuxing/Downloads/local.sql");
        sqlFileManager.setCheckModified(true);
        sqlFileManager.init();
    }

    @Test
    public void ref() throws Exception {
        Pair<String, List<String>> pair = new SqlTranslator(':').getPreparedSql(":res = call getUser(:id, :name)", Collections.emptyMap());
        System.out.println(pair.getItem1());
        System.out.println(pair.getItem2());
    }

    @Test
    public void trimTest() throws Exception {
        System.out.println("  data.sql\n\t    \n".trim());
    }

    @Test
    public void nutzIoc() throws Exception {
        XQLFileManager sqlFileManager = ioc.get(XQLFileManager.class, "sqlFileManager");
        sqlFileManager.init();
    }

    @Test
    public void strTest() throws Exception {
        String sql = "select * from user where 1=1 and id = 2)}];\n;; \t\n \r\n;; \n\n\t\r";
        System.out.println(SqlUtil.trimEnd(sql));
        System.out.println(sql.trim());
    }

    @Test
    public void bakiDao() throws Exception {
        XQLFileManager xqlFileManager = new XQLFileManager("xql-file-manager.old.yml");
        System.out.println(xqlFileManager);
    }
}
