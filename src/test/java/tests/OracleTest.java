package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.common.types.DataRow;
import rabbit.sql.Light;
import rabbit.sql.dao.Condition;
import rabbit.sql.dao.Filter;
import rabbit.sql.dao.LightDao;
import rabbit.sql.page.Pageable;
import rabbit.sql.page.impl.OraclePageHelper;

import java.util.Map;

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
