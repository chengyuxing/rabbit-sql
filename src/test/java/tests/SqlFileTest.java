package tests;

import org.junit.Test;
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
    public void sqlTest() throws Exception {
        SQLFileManager sqlFileManager = new SQLFileManager("pgsql/data.sql", "pgsql/other.sql");
        sqlFileManager.init();
        sqlFileManager.look();
        System.out.println("------------");

        String sql = sqlFileManager.get("pgsql.data.logical");
        System.out.println(sql);
        String dynamicSql = SqlUtil.dynamicSql(sql, Args.create()
                .add("name", "null")
                .add("age", null)
                .add("address", null)
                .add("id", null));
        System.out.println("-------------");
        System.out.println(dynamicSql);
    }

    @Test
    public void strTest() throws Exception {
        String sql = "select * from user where 1=1 and id = 2)}];\n;; \t\n \r\n;; \n\n\t\r";
//        System.out.println(SqlUtil.trimEnd(sql));
        System.out.println(sql.trim());
    }
}
