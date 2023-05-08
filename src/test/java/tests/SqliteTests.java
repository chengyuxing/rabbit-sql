package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.PagedResource;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedHashMap;

public class SqliteTests {

    private static HikariDataSource dataSource;
    private static Baki baki;

    @BeforeClass
    public static void init() {
        dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setJdbcUrl("jdbc:sqlite:/Users/chengyuxing/Downloads/sqlite_data");
        baki = BakiDao.of(dataSource);
    }

    @Test
    public void test1() {
        PagedResource<DataRow> resource = baki.query("select 1 union select 2")
                .pageable(1, 10)
                .collect();

        System.out.println(resource);
    }
}
