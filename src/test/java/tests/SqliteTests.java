package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.sql.utils.JdbcUtil;

public class SqliteTests {

    private static HikariDataSource dataSource;

    @BeforeClass
    public static void init() {
        dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setJdbcUrl("jdbc:sqlite:/Users/chengyuxing/Downloads/sqlite_data");
    }

    @Test
    public void test1() throws Exception {
        boolean a =JdbcUtil.supportStoredProcedure(dataSource.getConnection());
        System.out.println(a);
    }
}
