package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.common.types.DataRow;
import rabbit.sql.Light;
import rabbit.sql.dao.Args;
import rabbit.sql.dao.LightDao;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class SqliteTest {

    static Light light;

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

        light = LightDao.of(ds);
    }

    @Test
    public void test1() throws Exception {
        light.execute("create table user(id int primary key, name varchar(50), age int)");
    }

    @Test
    public void insert() throws Exception {
        light.insert("user", DataRow.fromList(Arrays.asList(2, "admin", 23), "id", "name", "age"));
    }

    @Test
    public void query() {
        light.query("select * from user where age > :age", Args.create().set("age", 25))
                .forEach(System.out::println);
    }
}
