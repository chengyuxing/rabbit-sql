package baki;

import baki.entity.User;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NewBakiTests {
    private static BakiDao bakiDao;
    private static Baki baki;

    @BeforeClass
    public static void init() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("new", "pgsql/new_for.sql");
        xqlFileManager.setPipes(Args.of("isOdd", "baki.pipes.IsOdd"));

        bakiDao = new BakiDao(dataSource);
        bakiDao.setXqlFileManager(xqlFileManager);
        baki = bakiDao;
    }

    @Test
    public void testBasicQuery() {
        QueryExecutor executor = baki.query("select * from test.user");
        executor.findFirst().ifPresent(System.out::println);
        System.out.println("-----");
        executor.stream().forEach(System.out::println);
        System.out.println("-----");
        User user = executor.findFirstEntity(User.class);
        System.out.println(user);
    }

    @Test
    public void testInsert() {
        User user = new User();
        user.setAge(28);
        user.setName("fake");
        user.setAddress("昆明市西山区");

        int i = baki.insert("test.user")
                .ignoreNull()
                .saveEntity(user);

        System.out.println(i);
    }

    @Test
    public void testUpdate() {
        User user = new User();
        user.setId(11);
        user.setAddress("昆明市西山区福海街道");
        baki.update("test.user", "id = :id")
                .ignoreNull()
                .saveEntity(user);
    }

    @Test
    public void testDel() {
        baki.delete("test.user")
                .arg("id", 10)
                .execute("id = :id");
    }

    @Test
    public void testDynamicSql1() {
        baki.query("&new.query")
                .arg("ids", Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
                .stream()
                .forEach(System.out::println);
    }

    @Test
    public void testDynamicSql2() {
        Args<Object> args = Args.create(
                "id", 11,
                "data", Args.create(
                        "name", "cyx",
                        "age", 23,
                        "address", "abc"
                )
        );
        baki.execute("&new.update", args);
    }

    @Test
    public void testDynamicSql3() {
        baki.query("&new.var")
                .stream()
                .forEach(System.out::println);
    }

    @Test
    public void testInsertEntity() {
        List<User> users = Stream.iterate(0, i -> i + 1)
                .limit(10)
                .map(i -> {
                    User user = new User();
                    user.setAddress("昆明" + i);
                    user.setDt(LocalDateTime.now());
                    user.setName("cyx");
                    user.setAge(i);
                    return user;
                }).collect(Collectors.toList());

        int i = baki.insert("test.user")
                .ignoreNull()
                .saveEntities(users);
        System.out.println(i);

    }
}
