package baki;

import baki.entity.User;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.types.OUTParamType;
import com.github.chengyuxing.sql.types.Param;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NewBakiTests {
    private static BakiDao bakiDao;
    private static Baki baki;

    @BeforeClass
    public static void init() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        XQLFileManager xqlFileManager = new XQLFileManager(Args.of("new", "pgsql/new_for.sql"));
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
        int i = baki.delete("test.user", "id = :id")
                .fast()
                .save(Arrays.asList(
                        Args.of("id", 2100037),
                        Args.of("id", 2100038))
                );
        System.out.println(i);
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
        Args<Object> args = Args.of(
                "id", 11,
                "data", Args.of(
                        "name", "cyx",
                        "age", 23,
                        "address", "kunming"
                )
        );
//        baki.execute("&new.update", args);
        int i = baki.update("test.user", "id = :id")
                .save(Args.of(
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
        List<Args<Object>> args = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            args.add(Args.of("users", Arrays.asList("chengyuxing", i, "昆明市", LocalDateTime.now())));
        }
        int i = baki.of("&new.insert").executeBatch(args);
        System.out.println(i);
    }

    @Test
    public void insertBatch() {
        List<Args<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            rows.add(Args.of("age", i, "name", "chengyuxing", "dt", LocalDateTime.now(), "address", "昆明市" + i));
        }

        int i;
        i = baki.insert("test.user").fast().save(rows);

        System.out.println(i);

    }

    @Test
    public void testCall() {
        baki.of("{call test.mvn_dependencies_query(:keyword)}")
                .call(Args.of("keyword", Param.IN("chengyuxing")))
                .<List<DataRow>>getFirstAs()
                .forEach(System.out::println);
    }

    @Test
    public void testCall3() {
        baki.query("select * from test.user")
                .pageable(1, 3)
                .collect()
                .getData()
                .forEach(System.out::println);
    }

    @Test
    public void testCall2() {
        baki.of("{:res = call test.sum(:a, :b)}")
                .call(Args.of("res", Param.OUT(OUTParamType.INTEGER))
                        .add("a", Param.IN(34))
                        .add("b", Param.IN(56)))
                .getOptional("res")
                .ifPresent(System.out::println);
    }

    @Test
    public void testArgs() {
        Args<Object> args = Args.of("name", "cyx", "age", 30, "date", "2023-8-4 22:45", "info", Args.of("address", "kunming"));
        args.updateValue("date", v -> DateTimes.toLocalDateTime(v.toString()));
        args.updateKey("name", "NAME");
        args.updateKeys(String::toUpperCase);
        System.out.println(args);
        System.out.println(DataRow.of());
    }

    @Test
    public void testArgs2() {

    }
}
