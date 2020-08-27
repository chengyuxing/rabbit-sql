package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class DbTests {

    static HikariDataSource dataSource;
    static HikariDataSource dataSource1;
    static HikariDataSource dataSource2;

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");
        dataSource.setDriverClassName("org.postgresql.Driver");

        dataSource1 = new HikariDataSource();
        dataSource1.setJdbcUrl("jdbc:sqlite:/Users/chengyuxing/Downloads/rabbit.db");
        dataSource1.setDriverClassName("org.sqlite.JDBC");

        dataSource2 = new HikariDataSource();
        dataSource2.setJdbcUrl("jdbc:oracle:thin:@192.168.1.115:1521/orcl");
        dataSource2.setDriverClassName("oracle.jdbc.OracleDriver");
        dataSource2.setUsername("nutzbook");
        dataSource2.setPassword("nutzbook");
    }

    @Test
    public void dbName() throws Exception {
    }
}
