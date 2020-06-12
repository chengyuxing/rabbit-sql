package tests;

import org.junit.Test;
import rabbit.sql.dao.SQLFileManager;

public class SqlFileTest {
    @Test
    public void sqlTest() throws Exception {
        SQLFileManager sqlFileManager = new SQLFileManager("pgsql/other.sql");
        sqlFileManager.init();
        sqlFileManager.look();

    }
}
