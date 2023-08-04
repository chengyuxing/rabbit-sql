package baki;

import baki.entity.User;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;

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
        Map<String, String> pipes = new HashMap<>();
        pipes.put("isOdd", "baki.pipes.IsOdd");
        xqlFileManager.setPipes(pipes);

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
        DataRow args = DataRow.of(
                "id", 11,
                "data", DataRow.of(
                        "name", "cyx",
                        "age", 23,
                        "address", "kunming"
                )
        );
//        baki.execute("&new.update", args);
        int i = baki.update("test.user", "id = :id")
                .save(DataRow.of(
                        "name", "cyx",
                        "age", 23,
                        "address", "kunming",
                        "id", 11
                ));
        System.out.println(i);
    }

    @Test
    public void testDynamicSql3() {
        baki.query("&new.var")
                .stream()
                .forEach(System.out::println);
    }

    @Test
    public void testInsertScript() {
        List<DataRow> args = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            args.add(DataRow.of("users", Arrays.asList("chengyuxing", i, "昆明市", LocalDateTime.now())));
        }
        int i = baki.of("&new.insert").executeBatch(args);
        System.out.println(i);
    }

    @Test
    public void insertBatch() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            rows.add(DataRow.of("age", i, "name", "chengyuxing", "dt", LocalDateTime.now(), "address", "昆明市" + i));
        }

        int i;
        i = baki.insert("test.user").fast().save(rows);

        System.out.println(i);

    }
}
