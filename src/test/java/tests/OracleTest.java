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

    @Test
    public void pageTest() throws Exception {
        Pageable<Map<String, Object>> data = light.query("select price,fruitName from fruit;\r\n;;\r\n;",
                DataRow::toMap,
                Condition.where(Filter.gt("price", 500)).desc("price"),
                OraclePageHelper.of(1, 100));
        System.out.println(data);
    }
}
