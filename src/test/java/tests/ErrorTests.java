package tests;

import com.zaxxer.hikari.HikariDataSource;
import com.github.chengyuxing.common.types.DataRow;
import com.github.chengyuxing.sql.dao.BakiDao;

import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ErrorTests {

    public static void main(String[] args) throws SQLException {
        HikariDataSource dataSource2 = new HikariDataSource();
        dataSource2.setJdbcUrl("jdbc:oracle:thin:@192.168.1.109:1521/orcl");
        dataSource2.setUsername("system");
        dataSource2.setPassword("system");
        dataSource2.setDriverClassName("oracle.jdbc.OracleDriver");
        dataSource2.setConnectionTimeout(250);
        BakiDao baki = BakiDao.of(dataSource2);

        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                try (Stream<DataRow> s = baki.query("select * from test.tb where blob is not null")) {
                    s.map(DataRow::toMap).collect(Collectors.toSet());
                } catch (Exception e) {
                    System.out.println("-------------" + e.getMessage());
                }
            }).start();
        }
    }
}
