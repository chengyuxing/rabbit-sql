package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;

public class SqliteTests {

    private static HikariDataSource dataSource;

    @BeforeClass
    public static void init() {
        dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setJdbcUrl("jdbc:sqlite:/Users/chengyuxing/Downloads/sqlite_data");
    }
}
