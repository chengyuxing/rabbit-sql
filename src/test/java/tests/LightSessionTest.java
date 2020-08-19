package tests;

import rabbit.common.types.DataRow;
import rabbit.sql.dao.*;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.sql.Light;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.stream.Stream;


public class LightSessionTest {

    static Light light;
    static LightDao orclLight;
    static HikariDataSource dataSource;

    @BeforeClass
    public static void init() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");
        dataSource.setDriverClassName("org.postgresql.Driver");
        light = LightDao.of(dataSource);

    }

    @Test
    public void dst() throws Exception {
        System.out.println(dataSource.getConnectionTimeout());
    }

    @Test
    public void streamTest() throws Exception {
        try (Stream<DataRow> fruits = orclLight.query("select * from fruit")) {
            fruits.limit(10).forEach(System.out::println);
        }
//        for(int i =0;i<20;i++){
//            Thread.sleep(3000);
//        }
    }

    @Test
    public void testStream2() throws Exception {
        orclLight.query("select * from fruit").limit(10)
                .forEach(System.out::println);

        for (int i = 0; i < 20; i++) {
            Thread.sleep(3000);
        }
    }

    @Test
    public void sqlTest() throws Exception {
//        boolean ex = orclLight.tableExists("chengyuxing.fruit");
//        System.out.println(ex);

        System.out.println(light.execute("drop table test.me"));
    }

    @Test
    public void oracleBlobTest() throws Exception {
        light.query("select * from nutzbook.files")
                .forEach(r -> {
                    String a = r.getString(0);
                });
    }

    @Test
    public void fileTest() throws Exception {
        light.query("select * from test.files")
                .forEach(r -> {
                    System.out.println(r);
                    byte[] bytes = r.get("file");
                    System.out.println(bytes.length);
//                    light.update("test.files", Params.builder()
//                                    .putIn("name", "cyx's file")
//                                    .build(),
//                            Cnd.New().where(Filter.eq("id", r.getInt("id"))));
                });
    }

    @Test
    public void jsonTest() throws Exception {
        light.query("select '{\n" +
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
                "    }'::jsonb")
                .forEach(r -> {
                    Object res = r.get(0);
                    System.out.println(r);
                    System.out.println(res);
                    System.out.println(res.getClass());
                });

        light.query("select t.a -> 'name', t.a -> 'age'\n" +
                "from (\n" +
                "         select '{\n" +
                "           \"name\": \"cyx\",\n" +
                "           \"age\": 26,\n" +
                "           \"address\": \"昆明市\"\n" +
                "         }'::json) t(a)")
                .forEach(System.out::println);

        light.fetch("select array [4,5] || array [1,2,3]").ifPresent(System.out::println);
    }

    @Test
    public void insert() throws Exception {
//        Transaction transaction = light.getTransaction();
        light.insert("test.user",
                ParamMap.create().putIn("name", "chengyuxing")
                        .putIn("password", "123456")
                        .putIn("id_card", "530111199305107030"));
//        transaction.commit();
    }

    @Test
    public void valueWrapTest() throws Exception {
        light.insert("test.score", ParamMap.create()
                .putIn("student_id", Wrap.wrapEnd(7, "::integer"))
                .putIn("subject", "政治")
                .putIn("grade", Wrap.wrapEnd("88", "::integer")));
    }

    @Test
    public void valueWrapTest2() throws Exception {
        light.update("test.score", ParamMap.create()
                        .putIn("grade", Wrap.wrapEnd("15", "::integer")),
                Condition.where(Filter.eq("id", Wrap.wrapEnd("7", "::integer")))
        );
    }

    @Test
    public void jsonTests() throws Exception {
        light.fetch("select '{\n" +
                "  \"a\": 1,\n" +
                "  \"b\": 2,\n" +
                "  \"c\": 3\n" +
                "}'::jsonb ??| array ['d', 's'];")
                .ifPresent(System.out::println);
    }

    @Test
    public void boolTest() throws Exception {
        light.fetch("select 'a',true,current_timestamp,current_date,current_time")
                .ifPresent(System.out::println);
    }

    @Test
    public void dateTimeTest() throws Exception {
        light.insert("test.datetime", ParamMap.create()
                .putIn("ts", LocalDateTime.now())
                .putIn("dt", LocalDate.now())
                .putIn("tm", LocalTime.now()));
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
