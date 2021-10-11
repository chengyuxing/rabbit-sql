package tests;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.SQLFileManager;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;
import org.nutz.dao.impl.NutDao;
import org.nutz.ioc.Ioc;
import org.nutz.ioc.impl.NutIoc;
import org.nutz.ioc.loader.json.JsonLoader;

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
        SQLFileManager sqlFileManager = new SQLFileManager();
        sqlFileManager.add("data", "pgsql/data.sql");
        sqlFileManager.init();
        System.out.println("-------");
        System.out.println(sqlFileManager.get("data.logical", Args.<Object>of("name", "cyx").add("age", 101)));

    }

    @Test
    public void sqlf() throws Exception {
        SQLFileManager sqlFileManager = new SQLFileManager();
        sqlFileManager.add("rabbit", "file:/Users/chengyuxing/Downloads/local.sql");
        sqlFileManager.init();
        sqlFileManager.look();
    }

    @Test
    public void ref() throws Exception {
        Pair<String, List<String>> pair = SqlUtil.getPreparedSql(":res = call getUser(:id, :name)", Collections.emptyMap());
        System.out.println(pair.getItem1());
        System.out.println(pair.getItem2());
    }

    @Test
    public void trimTest() throws Exception {
        System.out.println("  data.sql\n\t    \n".trim());
    }

    @Test
    public void nutzIoc() throws Exception {
        SQLFileManager sqlFileManager = ioc.get(SQLFileManager.class, "sqlFileManager");
        sqlFileManager.init();
        sqlFileManager.look();
    }

    @Test
    public void sqlTest() throws Exception {
        SQLFileManager sqlFileManager = new SQLFileManager("pgsql/nest.sql");
        sqlFileManager.setConstants(Args.of("db", "qbpt_deve"));
        sqlFileManager.setCheckModified(true);
//        sqlFileManager.add("pgsql/other.sql");
//        sqlFileManager.add("mac", "file:/Users/chengyuxing/Downloads/local.sql");

        sqlFileManager.init();
        sqlFileManager.look();
    }

    @Test
    public void strTest() throws Exception {
        String sql = "select * from user where 1=1 and id = 2)}];\n;; \t\n \r\n;; \n\n\t\r";
        System.out.println(SqlUtil.trimEnd(sql));
        System.out.println(sql.trim());
    }

    @Test
    public void bakiDao() throws Exception {
        BakiDao bakiDao = new BakiDao(null);
        NutDao nutDao = new NutDao();
    }
}
