package tests;

import org.junit.Test;
import rabbit.sql.dao.SQLFileManager;
import rabbit.sql.utils.SqlUtil;

public class SqlFileTest {
    @Test
    public void sqlTest() throws Exception {
        SQLFileManager sqlFileManager = new SQLFileManager("pgsql");
        sqlFileManager.init();
        sqlFileManager.look();

        System.out.println(sqlFileManager.get("other.other"));

    }

    @Test
    public void strTest() throws Exception {
        String sql = "select * from user where 1=1 and id = 2)}];\n;; \t\n \r\n;; \n\n\t\r";
//        System.out.println(SqlUtil.trimEnd(sql));
        System.out.println(sql.trim());
    }
}
