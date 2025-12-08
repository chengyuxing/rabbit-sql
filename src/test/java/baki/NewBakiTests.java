package baki;

import baki.entity.AnotherUser;
import baki.entity.User;
import baki.op.ExecuteCostWatcher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.exception.CheckViolationException;
import com.github.chengyuxing.sql.*;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.plugins.EntityFieldMapper;
import com.github.chengyuxing.sql.plugins.QueryCacheManager;
import com.github.chengyuxing.sql.plugins.QueryExecutor;
import com.github.chengyuxing.sql.transaction.Tx;
import com.github.chengyuxing.sql.types.StandardOutParamType;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.utils.JdbcUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.Column;
import java.lang.reflect.Field;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiFunction;
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
//        bakiDao.setSqlWatcher(new SqlWatcher.SqlWatchLogger());

        bakiDao.setQueryCacheManager(new QueryCacheManager() {

            @Override
            public @NotNull Stream<DataRow> get(@NotNull String sql, Map<String, ?> args, @NotNull RawQueryProvider provider) {
                return provider.query();
            }

            @Override
            public boolean isAvailable(@NotNull String sql, Map<String, ?> args) {
                return false;
            }
        });

        baki = bakiDao;

        bakiDao.setGlobalPageHelperProvider((databaseMetaData, dbName, namedParamPrefix) -> {
            if (dbName.equals("kingbasees")) {
                return new PGPageHelper();
            }
            return null;
        });
        bakiDao.setExecutionWatcher(new ExecuteCostWatcher());
        bakiDao.setEntityFieldMapper(new MyEntityFieldMapper());
    }

    static class MyEntityFieldMapper implements EntityFieldMapper {

        @Override
        public String apply(Field field) {
            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                return column.name();
            }
            return field.getName();
        }
    }

    @Test
    public void testNewDynamic() {
        try {
            baki.query("&new.new-dynamic").args("age", 34)
                    .rows()
                    .forEach(System.out::println);
        } catch (CheckViolationException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testPageQuery() {
        Map<String, XQLFileManagerConfig.Resource> res = bakiDao.getXqlFileManager().getResources();
        System.out.println(res);
        PagedResource<DataRow> rows = baki.query("&new.queryOneGuest")
                .args("page", 1, "size", 10)
                .pageable()
                .collect();
        System.out.println(rows);
    }

    @Test
    public void doPaging() {
        PagedResource<DataRow> resource = baki.query("select * from test.guest where id > :id")
                .args("id", 1999, "page", 1, "size", 10)
                .pageable()
                .collect();
        System.out.println(resource);
    }

    @Test
    public void testMoreRes() {
        Object res = baki.update("update test.guest set name = 'ccc' where id = :id", Args.of("id", 17));
        System.out.println(res);
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
        DataRow row = baki.execute("do\n" +
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
                "$$;", Args.of());
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
        DataRow res = baki.execute("create table test.temp(id int,content text,file bytea,dt timestamp)",
                Args.of("id", 1));
        System.out.println(res);
    }

    @Test
    public void testQu() {
        // language=sql
        baki.query("select * from test.guest")
                .stream()
//                .map(d -> d.toEntity(AnotherUser.class))
                .forEach(d -> {
                    System.out.println(d);
                });
    }

    @Test
    public void testCallA() {
        Tx.using(() -> {
            DataRow res = baki.call("{call getGuestBy(:w, :res)}",
                    Args.of("w", Param.IN("id = 1"))
                            .add("res", Param.OUT(StandardOutParamType.REF_CURSOR)));
            System.out.println(res);
        });
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
    public void testInsertReturning() {
        DataRow res = baki.execute("insert into test.guest(name, address, age, photo)\n" +
                "values ('xxx', 'kunming', 37, :photo)\n" +
                "returning id", Args.of("photo", new FileResource("log4j.properties").getInputStream()));
        System.out.println(res);
    }

    @Test
    public void testUpdate2() {
        DataRow res = baki.execute("&new.update", Args.of(
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
        for (int i = 0; i < 10; i++) {
            args.add(Args.of("users", Arrays.asList("chengyuxing", i, "昆明市")));
        }
        int i = baki.execute("&new.insert", args);
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
        Object res = baki.call("{call test.mvn_dependencies_query(:keyword)}",
                        Args.of("keyword", Param.IN("chengyuxing")))
                .<List<DataRow>>getAs(0);

        System.out.println(res);
    }

    @Test
    public void testCall3() {
        baki.query("&new.qqq")
                .args("page", 1, "size", 4, "id", 10)
                .arg("orderBy", "order by id desc")
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
        baki.call("{:res = call test.sum(:a, :b)}",
                        Args.of("res", Param.OUT(StandardOutParamType.INTEGER))
                                .add("a", Param.IN(34))
                                .add("b", Param.IN(9956)))
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
        baki.execute("select current_date", Args.of())
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
