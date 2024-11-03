package baki;

import baki.entity.AnotherUser;
import baki.entity.Guest;
import baki.entity.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.*;
import com.github.chengyuxing.sql.dsl.types.StandardOperator;
import com.github.chengyuxing.sql.plugins.SqlWatcher;
import com.github.chengyuxing.sql.support.executor.QueryExecutor;
import com.github.chengyuxing.sql.transaction.Tx;
import com.github.chengyuxing.sql.types.StandardOutParamType;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

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

//        bakiDao.setQueryCacheManager(new QueryCacheManager() {
//            @Override
//            public Stream<DataRow> get(String key) {
//                try {
//                    Path path = Paths.get("/Users/chengyuxing/Downloads/" + key + ".data");
//                    if (!Files.exists(path)) {
//                        return null;
//                    }
//                    List<DataRow> cache = json.readerForListOf(DataRow.class)
//                            .readValue(Files.newInputStream(path));
//                    return cache.stream();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public void put(String key, List<DataRow> value) {
//                try {
//                    byte[] cache = json.writeValueAsBytes(value);
//                    Files.write(Paths.get("/Users/chengyuxing/Downloads/" + key + ".data"), cache);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public boolean isAvailable(String sql, Map<String, Object> args) {
//                return true;
//            }
//        });

        bakiDao.setOperatorWhiteList(new HashSet<>(Arrays.asList("~")));

        baki = bakiDao;
    }

    @Test
    public void proxyTest1() throws IllegalAccessException {
        HomeMapper homeMapper = bakiDao.proxyXQLMapper(HomeMapper.class);
//        homeMapper.queryAllGuests().forEach(System.out::println);
        PagedResource<DataRow> res = homeMapper.queryAllGuests(1, 3);
        System.out.println(res);
//        int i = homeMapper.now();
//        System.out.println(i);
    }

    @Test
    public void testProcyTest2() throws IllegalAccessException {
        HomeMapper homeMapper = bakiDao.proxyXQLMapper(HomeMapper.class);
        DataRow row = homeMapper.mavenDependenciesQuery(Param.IN("chengyuxing"));
        System.out.println(row);
        DataRow row1 = homeMapper.sum(Param.IN(10), Param.IN(31), Param.OUT(StandardOutParamType.INTEGER));
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
    public void testQu() {
        // language=sql
        baki.query("select * from test.guest")
                .stream()
                .map(d -> d.toEntity(AnotherUser.class))
                .forEach(System.out::println);
    }

    @Test
    public void testDslQuery() {
        // FIXME ORDER BY 要不要加下动态sql功能
        baki.query(Guest.class)
                .where(w -> w.isNotNull(Guest::getId)
                        .gt(Guest::getId, 1)

                        .and(and -> and.in(Guest::getId, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8))
                                .startsWith(Guest::getName, "cyx")
                                .or(s -> s.between(Guest::getAge, 1, 100).notBetween(Guest::getAge, 100, 1000))
                                .in(Guest::getName, Arrays.asList("cyx", "jack"))
                        )

                        .or(o -> o.isNotNull(Guest::getId))
                        .of(Guest::getAddress, () -> "~", "kunming")
                        .neq(Guest::getAddress, "beijing", Objects::nonNull)

                        .and(a -> a.or(b -> b.lt(Guest::getAge, 30).eq(Guest::getAge, 30)))
                        .eq(Guest::getAddress, "beijing")

                        .or(o -> o.gt(Guest::getAge, 10).lt(Guest::getAge, 30))
                        .eq(Guest::getAddress, "beijing")
                        .peek((a, b) -> System.out.println(a))
                )

                .groupBy(g -> g.count()
                        .max(Guest::getAge)
                        .avg(Guest::getAge)
                        .by(Guest::getAge)
                        .having(h -> h.count(StandardOperator.GT, 1))
                )
                .orderBy(o -> o.asc(Guest::getAge))
//                .orderBy(o -> o.desc(Guest::getAge).asc(Guest::getAddress))
//                .orderBy(o -> o.desc("name"))
                .toList()
                .forEach(System.out::println)
        ;
    }

    @Test
    public void testDslGroupBy() {
        PagedResource<DataRow> res = baki.query(Guest.class)
                .where(w -> w.gt(Guest::getAge, 23))
                .groupBy(g -> g.count(Guest::getId).max(Guest::getAge).by(Guest::getAge)
                        .having(h -> h.min(Guest::getId, StandardOperator.IS_NOT_NULL, 18980)))
                .groupBy(g -> g.sum(Guest::getAge)
                        .by(Guest::getName)
                        .having(h -> h.sum(Guest::getAge, StandardOperator.GT, 935))
                        .having(h -> h.max(Guest::getAddress, StandardOperator.EQ, "kunming")
                                .or(o -> o.min(Guest::getAge, StandardOperator.GT, 28)
                                        .max(Guest::getAge, StandardOperator.EQ, 1)))
                )
                .orderBy(o -> o.asc(Guest::getAge))
                .toPagedRowResource(1, 5);
        System.out.println(res);
    }

    @Test
    public void testDlsQuery2() {
        baki.query(Guest.class)
                .where(w -> w.and(o -> o.lt(Guest::getAge, 15)
                                .gt(Guest::getAge, 60))
                        .eq(Guest::getName, "cyx")

                        .peek((sql, arg) -> System.out.println(sql))
                )
                .toList();

    }

    @Test
    public void testDslQuery3() {
        baki.query(Guest.class)
                .where(w -> w.and(o -> o.or(a -> a.eq(Guest::getName, "cyx")
                                                .eq(Guest::getAge, 30))
                                        .or(r -> r.eq(Guest::getName, "jack")
                                                .eq(Guest::getAge, 60))
                                )
                                .peek((sql, arg) -> System.out.println(sql))
                )
                .toList();
    }

    @Test
    public void testDslUpdate() {
        Guest guest = new Guest();
//        guest.setId(1);
        guest.setAddress("USAbc");
        int i = baki.update(guest)
                .ignoreNull()
                .by(w -> w.eq(Guest::getId, 3));
        System.out.println(i);
    }

    @Test
    public void testCallA() {
        Tx.using(() -> {
            DataRow res = baki.of("{call getGuestBy(:w, :res)}")
                    .call(Args.of("w", Param.IN("id = 1"))
                            .add("res", Param.OUT(StandardOutParamType.REF_CURSOR)));
            System.out.println(res);
        });
    }

    @Test
    public void testDslInsert() {
        baki.insert(new Guest());
    }

    @Test
    public void testPivot() {
//        Object res = baki.query("select * from test.student")
//                .pivot("name", "subject", (k, rows) -> {
//                    return rows.stream().map(d -> d.getInt("score")).findFirst().orElse(0);
//                });
//        System.out.println(res);
    }

    @Test
    public void testUpdateEntity() {
        AnotherUser user = new AnotherUser();
        user.setNl(76);
//        user.setXm("cyx");
        user.setUserId(2120056);
        baki.update(AnotherUser.class)
                .ignoreNull()
//                .save(user, Where.of().eq("id", 1))
        ;
//        System.out.println(i);
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
//        int i = baki.delete(AnotherUser.class)
//                .save(user, Where.of().eq("id", 1));
//        System.out.println(i);
    }

    @Test
    public void testUpdate() {
        Guest user = new Guest();
        user.setId(11);
        user.setAddress("昆明市西山区福海街道");
        baki.update(User.class)
                .ignoreNull()
                .byId();
    }

    @Test
    public void testDel() {
//        int i = baki.delete(User.class)
//                .save(Arrays.asList(
//                        Args.of("id", 2100037),
//                        Args.of("id", 2100038))
//                );
//        System.out.println(i);
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

//        int i = baki.update("test.user", "id = :id")
//                .save(Args.of(
//                        "name", "cyx",
//                        "age", 23,
//                        "address", "kunming",
//                        "id", 11
//                ));
//        System.out.println(i);
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
//        i = baki.insert("test.user").save(rows);
//
//        System.out.println(i);

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
                .arg("orderBy", "id desc")
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
                .call(Args.of("res", Param.OUT(StandardOutParamType.INTEGER))
                        .add("a", Param.IN(34))
                        .add("b", Param.IN(56)))
                .getOptional("res")
                .ifPresent(System.out::println);
    }

    @Test
    public void testQ() throws SQLException {
//        baki.query("select id as \"ID\",name as \"NAME\",age,address from test.user limit 5").zip()
//                .forEach((k, v) -> System.out.println(k + " -> " + v));

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
//        int i = baki.insert("test.user")
//                .save(args);
//        System.out.println(i);
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
