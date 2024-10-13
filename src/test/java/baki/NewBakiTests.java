package baki;

import baki.entity.AnotherUser;
import baki.entity.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.*;
import com.github.chengyuxing.sql.support.QueryCacheManager;
import com.github.chengyuxing.sql.support.SqlWatcher;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.types.OUTParamType;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;

public class NewBakiTests {
    private static BakiDao bakiDao;
    private static Baki baki;
    private static final ObjectMapper json = new ObjectMapper();

    @BeforeClass
    public static void init() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("new", "pgsql/new_for.sql");
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
        bakiDao.setSqlWatcher(new SqlWatcher.SqlWatchLogger());

        bakiDao.setQueryCacheManager(new QueryCacheManager() {
            @Override
            public Stream<DataRow> get(String key) {
                try {
                    Path path = Paths.get("/Users/chengyuxing/Downloads/" + key + ".data");
                    if (!Files.exists(path)) {
                        return null;
                    }
                    List<DataRow> cache = json.readerForListOf(DataRow.class)
                            .readValue(Files.newInputStream(path));
                    return cache.stream();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void put(String key, List<DataRow> value) {
                try {
                    byte[] cache = json.writeValueAsBytes(value);
                    Files.write(Paths.get("/Users/chengyuxing/Downloads/" + key + ".data"), cache);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean isAvailable(String sql, Map<String, Object> args) {
                return true;
            }
        });

        baki = bakiDao;
    }

    @Test
    public void proxyTest1() throws IllegalAccessException {
        HomeMapper homeMapper = bakiDao.proxyXQLMapper(HomeMapper.class);
//        homeMapper.queryAllGuests().forEach(System.out::println);
        int i = homeMapper.now();
        System.out.println(i);
    }

    @Test
    public void testProcyTest2() throws IllegalAccessException {
        HomeMapper homeMapper = bakiDao.proxyXQLMapper(HomeMapper.class);
        DataRow row = homeMapper.mavenDependenciesQuery(Param.IN("chengyuxing"));
        System.out.println(row);
        DataRow row1 = homeMapper.sum(Param.IN(10), Param.IN(31), Param.OUT(OUTParamType.INTEGER));
        System.out.println(row1);
    }

    @Test
    public void testPlsql() {
        DataRow row = baki.of("do\n" +
                "$$\n" +
                "    declare\n" +
                "        x    integer[];\n" +
                "        nums integer[] := array [[1,2,3],[4,5,6],[7,8,9]];\n" +
                "    begin\n" +
                "        foreach x slice 1 in array nums\n" +
                "            loop\n" +
                "                raise warning 'num:%',x;\n" +
                "            end loop;\n" +
                "    end;\n" +
                "\n" +
                "$$;").execute();
        System.out.println(row);
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
    public void testUpdate2() {
        DataRow res = baki.of("&new.update").execute(Args.of(
                "id", 11,
                "sets", Args.of(
                        "name", "chengyuxing",
                        "age", 31,
                        "address", "kunming",
                        "photo", new FileResource("file:///Users/chengyuxing/Downloads/niwo.png").getInputStream()
                )
        ));
        System.out.println(res);
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
        Object res = baki.of("{call test.mvn_dependencies_query(:keyword)}")
                .call(Args.of("keyword", Param.IN("chengyuxing")));

        System.out.println(res);
    }

    @Test
    public void testCall3() {
        baki.query("&new.qqq")
                .args("page", 1, "size", 4)
                .pageable()
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
