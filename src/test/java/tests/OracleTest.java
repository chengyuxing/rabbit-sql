package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import rabbit.sql.Light;
import rabbit.sql.dao.LightDao;

public class OracleTest {
    static Light light;

    @BeforeClass
    public static void init() {
        HikariDataSource dataSource2 = new HikariDataSource();
        dataSource2.setJdbcUrl("jdbc:oracle:thin:@127.0.0.1:1521/orcl");
        dataSource2.setUsername("chengyuxing");
        dataSource2.setPassword("123456");
        dataSource2.setDriverClassName("oracle.jdbc.OracleDriver");
        light = LightDao.of(dataSource2);
    }
}
