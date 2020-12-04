package tests;

import org.junit.Test;
import org.nutz.ioc.Ioc;
import org.nutz.ioc.impl.NutIoc;
import org.nutz.ioc.loader.json.JsonLoader;
import rabbit.common.tuple.Pair;
import rabbit.sql.dao.Args;
import rabbit.sql.dao.SQLFileManager;
import rabbit.sql.utils.SqlUtil;

import java.util.List;

public class SqlFileTest {

    @Test
    public void ref() throws Exception {
        Pair<String, List<String>> pair = SqlUtil.getPreparedSql(":res = call getUser(:id, :name)");
        System.out.println(pair.getItem1());
        System.out.println(pair.getItem2());
    }

    @Test
    public void trimTest() throws Exception {
        System.out.println("  data.sql\n\t    \n".trim());
    }

    @Test
    public void nutzIoc() throws Exception {
        Ioc ioc = new NutIoc(new JsonLoader("ioc.js"));
        SQLFileManager sqlFileManager = ioc.get(SQLFileManager.class, "sqlFileManager");
        sqlFileManager.init();
        sqlFileManager.look();
    }

    @Test
    public void sqlTest() throws Exception {
        SQLFileManager sqlFileManager = new SQLFileManager("pgsql/data.sql");
        sqlFileManager.addNamedPath("cyx", "pgsql/other.sql");
        sqlFileManager.addNamedPath("mac", "file:/Users/chengyuxing/Downloads/local.sql");
        sqlFileManager.init();
        sqlFileManager.look();
    }

    @Test
    public void strTest() throws Exception {
        String sql = "select * from user where 1=1 and id = 2)}];\n;; \t\n \r\n;; \n\n\t\r";
//        System.out.println(SqlUtil.trimEnd(sql));
        System.out.println(sql.trim());
    }
}
