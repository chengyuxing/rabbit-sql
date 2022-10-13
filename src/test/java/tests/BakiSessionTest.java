package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.time.*;
import java.util.Arrays;
import java.util.stream.Stream;


public class BakiSessionTest {

    static BakiDao baki;
    static BakiDao orclLight;
    static HikariDataSource dataSource;

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");
        dataSource.setDriverClassName("org.postgresql.Driver");
        baki = BakiDao.of(dataSource);
        baki.setCheckParameterType(false);
        XQLFileManager sqlFileManager = new XQLFileManager(Args.of("nest", "pgsql/nest.sql"));
        sqlFileManager.setConstants(Args.of("db", "test"));
        baki.setXqlFileManager(sqlFileManager);
        baki.setNamedParamPrefix('?');
    }

    @Test
    public void testinsert() throws Exception {
        baki.insert("test.tb")
                .save(Args.of("str", Args.of("a", "b")));
    }

    @Test
    public void testA() throws Exception {
        baki.query("select now(), array ['a','b','c']").findFirst()
                .ifPresent(System.out::println);
    }

    @Test
    public void upd() throws Exception {
        int i = baki.update("${db}.region", "name = ?oldName")
                .safe()
                .save(Args.of("name", "南亚风情第一城").add("oldName", "南亚风情园").add("abc", "123"));
        System.out.println(i);
    }

    @Test
    public void testQuery() throws Exception {
        PagedResource<DataRow> resource = baki.query("select * from test.region where id < ?id")
                .arg("id", 8)
                .<DataRow>pageable(1, 7)
                .count(5)
                .collect(d -> d);
        System.out.println(resource);

        try (Stream<DataRow> s = baki.query("select * from test.region where id < ?id")
                .arg("id", 10)
                .arg("name", "cyx")
                .stream()) {
            s.forEach(System.out::println);
        }

        baki.query("select * from test.region where id = ?id")
                .arg("id", 10)
                .findFirst()
                .ifPresent(System.out::println);

        System.out.println(baki.query("select 1 from test.region where id = 109").exists());
    }

    @Test
    public void engine() throws Exception {
        baki.query("select * from ${db}.history where length(words) < ?num or words ~ ?regex")
                .args(Args.<Object>of("num", 5).add("regex", "^tran"))
                .maps()
                .forEach(System.out::println);
    }

    @Test
    public void testX() throws Exception {
        baki.query("select '{\"a\":\"cyx\"}'::jsonb as x").findFirst().ifPresent(d -> {
            Object v = d.get("x");
            System.out.println(d.getType(0));
            System.out.println(ReflectUtil.obj2Json(d.get("x")));
        });
    }

    @Test
    public void meta() throws Exception {
        DatabaseMetaData metaData = baki.using(connection -> {
            try {
                return connection.getMetaData();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        System.out.println(metaData.getJDBCMajorVersion());
        System.out.println(metaData.getJDBCMinorVersion());
        System.out.println(metaData.getDriverVersion());
        System.out.println(metaData.getDriverMajorVersion());
        System.out.println(metaData.getDriverMinorVersion());
        System.out.println(metaData.getDatabaseProductVersion());
        System.out.println(metaData.getDriverName());
        System.out.println(metaData.getDatabaseProductName());
        System.out.println(metaData.getConnection().getCatalog());
    }

    @Test
    public void dst() throws Exception {
        DataRow res = baki.execute("create table test.tx(a int)");
//        DataRow res = baki.execute("drop table test.tx");
        System.out.println(res);
    }

    @Test
    public void streamTest() throws Exception {
        try (Stream<DataRow> fruits = orclLight.query("select * from fruit").stream()) {
            fruits.limit(10).forEach(System.out::println);
        }
//        for(int i =0;i<20;i++){
//            Thread.sleep(3000);
//        }
    }

    @Test
    public void testStream2() throws Exception {
        orclLight.query("select * from fruit").stream().limit(10)
                .forEach(System.out::println);

        for (int i = 0; i < 20; i++) {
            Thread.sleep(3000);
        }
    }

    @Test
    public void sqlTest() throws Exception {
//        boolean ex = orclLight.tableExists("chengyuxing.fruit");
//        System.out.println(ex);

        System.out.println(baki.execute("drop table test.me"));
    }

    @Test
    public void oracleBlobTest() throws Exception {
        baki.query("select * from nutzbook.files").stream()
                .forEach(r -> {
                    String a = r.getFirstAs();
                });
    }

    @Test
    public void fileTest() throws Exception {
        baki.query("select * from test.files").stream()
                .forEach(r -> {
                    System.out.println(r);
                    byte[] bytes = r.getAs("file");
                    System.out.println(bytes.length);
//                    baki.update("test.files", Args.builder()
//                                    .putIn("name", "cyx's file")
//                                    .build(),
//                            Cnd.New().where(Filter.eq("id", r.getInt("id"))));
                });
    }

    @Test
    public void jsonTest() throws Exception {
        baki.query("select '{\n" +
                        "      \"guid\": \"9c36adc1-7fb5-4d5b-83b4-90356a46061a\",\n" +
                        "      \"name\": \"Angela Barton\",\n" +
                        "      \"is_active\": true,\n" +
                        "      \"company\": \"Magnafone\",\n" +
                        "      \"address\": \"178 Howard Place, Gulf, Washington, 702\",\n" +
                        "      \"registered\": \"2009-11-07T08:53:22 +08:00\",\n" +
                        "      \"latitude\": 19.793713,\n" +
                        "      \"longitude\": 86.513373,\n" +
                        "      \"tags\": [\n" +
                        "        \"enim\",\n" +
                        "        \"aliquip\",\n" +
                        "        \"qui\"\n" +
                        "      ]\n" +
                        "    }'::jsonb").stream()
                .forEach(r -> {
                    Object res = r.get(0);
                    System.out.println(r);
                    System.out.println(res);
                    System.out.println(res.getClass());
                });

        baki.query("select t.a -> 'name', t.a -> 'age'\n" +
                        "from (\n" +
                        "         select '{\n" +
                        "           \"name\": \"cyx\",\n" +
                        "           \"age\": 26,\n" +
                        "           \"address\": \"昆明市\"\n" +
                        "         }'::json) t(a)")
                .stream()
                .forEach(System.out::println);

        baki.query("select array [4,5] || array [1,2,3]").findFirst().ifPresent(System.out::println);
    }

    @Test
    public void jsonTests() throws Exception {
        baki.query("select '{\n" +
                        "  \"a\": 1,\n" +
                        "  \"b\": 2,\n" +
                        "  \"c\": 3\n" +
                        "}'::jsonb ??| array ['d', 's'];")
                .findFirst()
                .ifPresent(System.out::println);
    }

    @Test
    public void batchExe() throws Exception {
        int[] res = baki.batchExecute(
                "create table test.t_a(id int)",
                "create table test.t_b(id int)",
                "create table test.t_c(id int)",
                "create table test.t_d(id int)",
                "create table test.t_e(id int)",
                "create table test.t_f(id int)"
        );
        System.out.println(Arrays.toString(res));
    }

    @Test
    public void boolTest() throws Exception {
        baki.query("select 'a',true,current_timestamp,current_date,current_time")
                .findFirst()
                .ifPresent(System.out::println);
    }

    @Test
    public void update() throws Exception {
//        baki.update("test.history",
//                Args.of("words", "chengyuxingo"),
//                Condition.where("userid = :id").addArg("id", "a036313a-21f4-48ff-8308-532a6d62e5e6"));
    }

    @Test
    public void del() throws Exception {
//        baki.delete("test.history", Condition.where(Filter.eq("userid", "eb9e6bbd-b750-4640-b1aa-14a813150fd7")));
    }

    @Test
    public void caseU() throws Exception {
        baki.query("select words, userid, DEL as \"DeL\"\n" +
                        "from test.history")
                .stream()
                .forEach(System.out::println);
    }

    @Test
    public void secondsTest() throws Exception {
        System.out.println(new Timestamp(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));

        System.out.println(new Date(LocalDate.now().atStartOfDay(ZoneOffset.systemDefault()).toInstant().toEpochMilli()));

        System.out.println(new Time(LocalTime.now().atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));

        System.out.println(Instant.now().atZone(ZoneId.systemDefault()).toLocalDateTime());

        System.out.println(new Timestamp(Instant.now().toEpochMilli()));
    }

    @Test
    public void strTest() throws Exception {
        String sql = "mm||mm";
        System.out.println("\"" + sql + "\"");
    }

}
