package tests;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SqliteTest {

    static BakiDao baki;

    @BeforeClass
    public static void init() throws IOException {
        String dsPath = "/Users/chengyuxing/test/data.rabbit";
        File file = new File(dsPath);
        if (!file.exists()) {
            if (file.createNewFile()) {
                System.out.println("创建数据文件成功！");
            }
        }

        HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName("org.sqlite.JDBC");
        ds.setJdbcUrl("jdbc:sqlite:" + dsPath);

        baki = new BakiDao(ds);
    }


    @Test
    public void query() {
        baki.query("select * from user where age > :age")
                .args(Args.create().add("age", 25))
                .stream()
                .forEach(System.out::println);
    }
}
