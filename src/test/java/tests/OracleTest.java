package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import com.github.chengyuxing.sql.BakiDao;

public class OracleTest {
    static BakiDao baki;

    @BeforeClass
    public static void init() {
        HikariDataSource dataSource2 = new HikariDataSource();
        dataSource2.setJdbcUrl("jdbc:oracle:thin:@192.168.1.109:1521/orcl");
        dataSource2.setUsername("system");
        dataSource2.setPassword("system");
        dataSource2.setDriverClassName("oracle.jdbc.OracleDriver");
        baki = BakiDao.of(dataSource2);
    }

    @Test
    public void sss() throws Exception{
        baki.fetch("select 1 A, 2 \"B\", sysdate DT, sys_guid() \"GuID\" from dual")
        .ifPresent(System.out::println);
    }
}
