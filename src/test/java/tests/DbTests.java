package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.common.types.DataRow;
import rabbit.sql.utils.JdbcUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

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

//        dataSource1 = new HikariDataSource();
//        dataSource1.setJdbcUrl("jdbc:sqlite:/Users/chengyuxing/Downloads/rabbit.db");
//        dataSource1.setDriverClassName("org.sqlite.JDBC");
//
//        dataSource2 = new HikariDataSource();
//        dataSource2.setJdbcUrl("jdbc:oracle:thin:@192.168.1.115:1521/orcl");
//        dataSource2.setDriverClassName("oracle.jdbc.OracleDriver");
//        dataSource2.setUsername("nutzbook");
//        dataSource2.setPassword("nutzbook");
    }

    @Test
    public void dbName() throws Exception {

    }

    @Test
    public void execute() throws Exception {
        // 思路可以返回一个DataRow
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement("create index tb_idx on test.tb(a)");
        boolean res = statement.execute();
        if (res) {
            ResultSet resultSet = statement.getResultSet();
            List<DataRow> rows = JdbcUtil.createDataRows(resultSet,"", -1);
            System.out.println(rows);
        } else {
            int count = statement.getUpdateCount();
            System.out.println(count);
        }
    }
}
