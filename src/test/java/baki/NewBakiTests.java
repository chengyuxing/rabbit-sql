package baki;

import baki.entity.AnotherUser;
import baki.entity.User;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.support.SqlInterceptor;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.types.OUTParamType;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.Variable;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.github.chengyuxing.sql.utils.SqlUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.checkerframework.checker.units.qual.A;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.SQLException;
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
        xqlFileManager.setConstants(Args.of("db", "test"));

        bakiDao = new BakiDao(dataSource);
        bakiDao.setXqlFileManager(xqlFileManager);

        bakiDao.setStatementValueHandler((ps, index, value, metaData) -> {
            if (ps instanceof CallableStatement) {
                System.out.println("Procedure calling.");
            }
            JdbcUtil.setStatementValue(ps, index, value);
        });

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
    public void testInterceptor() {
        DataRow res = baki.of("create table test.temp(id int,content text,file bytea,dt timestamp)")
                .execute();
        System.out.println(res);
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
    public void testQu() {
        baki.query("select * from test.user")
                .stream()
                .map(d -> d.toEntity(AnotherUser.class))
                .forEach(System.out::println);
    }

    @Test
    public void testInsertEntity() {
        AnotherUser user = new AnotherUser();
        user.setNl(322);
        user.setXm("cyx");
        user.setXxdz("kunming xi shan qu");
        int i = baki.entity(AnotherUser.class)
                .insert()
                .ignoreNull()
                .saveEntity(user);
        System.out.println(i);
    }

    @Test
    public void testUpdateEntity() {
        AnotherUser user = new AnotherUser();
        user.setNl(76);
//        user.setXm("cyx");
        user.setUserId(2120056);
        int i = baki.entity(AnotherUser.class)
                .update("id = :id")
                .ignoreNull()
                .saveEntity(user);
        System.out.println(i);
    }

    @Test
    public void testDeleteEntity() {
        AnotherUser user = new AnotherUser();
        user.setUserId(2120056);
        int i = baki.entity(AnotherUser.class)
                .delete("id = :id")
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
    public void testQueryTemp() {
        baki.query("&new.queryTemp")
                .arg("tableName", "user")
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
//        DataRow row = baki.of("&new.update").execute(args);

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
    public void testPageTo() {
        DataRow row = baki.query("select * from test.user")
                .pageable(1, 4)
                .rewriteDefaultPageArgs(a -> {
                    a.updateKey("limit", "my_limit");
                    return a;
                })
                .collect()
                .to((pager, data) -> DataRow.of(
                        "length", pager.getRecordCount(),
                        "data", data)
                );
        System.out.println(row);
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
    public void testQ() throws SQLException {
        baki.query("select id as \"ID\",name as \"NAME\",age,address from test.user limit 5").zip()
                .forEach((k, v) -> System.out.println(k + " -> " + v));

    }

    @Test
    public void testS() {
        baki.of("select current_date").execute()
                .forEach((k, v) -> {
                    System.out.println(k + " -> " + v);
                });
    }

    @Test
    public void testA() {
        baki.query("select 1,2,3,4,5").findFirst()
                .ifPresent(System.out::println);
    }

    @Test
    public void testPatchError() {
        List<Args<Object>> args = new ArrayList<>();
        args.add(Args.of("name", "chengyuxing", "dt", LocalDateTime.now()));
        args.add(Args.of("name", "chengyuxing", "dt", 1000L));
        args.add(Args.of("name", "chengyuxing", "dt", LocalDateTime.now()));
        int i = baki.insert("test.user")
                .fast()
                .save(args);
        System.out.println(i);
    }

    @Test
    public void testLineAnno() {
        baki.query("select name, age, address\n" +
                        "from test.user\n" +
                        "where name = :name\n" +
                        "--or age = :age\n" +
                        "   or address = :address")
                .args("name", "cyx", "age", 27, "address", "kunming")
                .findFirst()
                .ifPresent(System.out::println);
    }
}
