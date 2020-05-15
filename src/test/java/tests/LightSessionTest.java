package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rabbit.sql.dao.*;
import org.rabbit.sql.Light;
import org.rabbit.sql.dao.Params;
import org.rabbit.sql.types.ValueWrap;

public class LightSessionTest {

    static Light light;
    static Light orclLight;

    @BeforeClass
    public static void init() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");
        dataSource.setDriverClassName("org.postgresql.Driver");
        light = LightDao.of(dataSource);

//        HikariDataSource dataSource2 = new HikariDataSource();
//        dataSource2.setJdbcUrl("jdbc:oracle:thin:@192.168.101.4:1521/orcl");
//        dataSource2.setUsername("chengyuxing");
//        dataSource2.setPassword("123456");
//        dataSource2.setDriverClassName("oracle.sql.OracleDriver");
//        orclLight = LightDao.of(dataSource2);
    }

    @Test
    public void sqlTest() throws Exception {
//        boolean ex = orclLight.tableExists("chengyuxing.fruit");
//        System.out.println(ex);

        System.out.println(light.execute("drop table test.me"));
    }

    @Test
    public void oracleBlobTest() throws Exception {
        light.query("select * from nutzbook.files", r -> r)
                .forEach(r -> {
                    String a = r.getString(0);
                });
    }

    @Test
    public void fileTest() throws Exception {
        light.query("select * from test.files", r -> r)
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
                "    }'::jsonb", r -> r)
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
                "         }'::json) t(a)", r -> r)
                .forEach(System.out::println);

        light.fetch("select array [4,5] || array [1,2,3]", r -> r).ifPresent(System.out::println);
    }

    @Test
    public void testQuery() throws Exception {
        light.query("select * from test.user",
                r -> r,
                Condition.New().where(Filter.startsWith("name", "c"))
                        .and(Filter.gt("id", ValueWrap.wrapEnd("1000", "::integer"))))
                .forEach(System.out::println);
    }

    @Test
    public void insert() throws Exception {
//        Transaction transaction = light.getTransaction();
        light.insert("test.user",
                Params.builder().putIn("name", "chengyuxing")
                        .putIn("password", "123456")
                        .putIn("id_card", "530111199305107030")
                        .build());
//        transaction.commit();
    }

    @Test
    public void valueWrapTest() throws Exception {
        light.insert("test.score", Params.builder()
                .putIn("student_id", ValueWrap.wrapEnd(7, "::integer"))
                .putIn("subject", "物理")
                .putIn("grade", ValueWrap.wrapEnd("31", "::integer"))
                .build());
    }

    @Test
    public void valueWrapTest2() throws Exception {
        light.update("test.score", Params.builder()
                        .putIn("student_id", ValueWrap.wrapEnd("5", "::integer"))
                        .build(),
                Condition.New().where(Filter.eq("id", ValueWrap.wrapEnd("11", "::integer"))));
    }

    @Test
    public void jsonTests() throws Exception {
        light.fetch("select '{\n" +
                "  \"a\": 1,\n" +
                "  \"b\": 2,\n" +
                "  \"c\": 3\n" +
                "}'::jsonb ??| array ['d', 's'];", r -> r)
                .ifPresent(System.out::println);
    }

    @Test
    public void strTest() throws Exception{
        String sql = "mm||mm";
        System.out.println("\""+sql+"\"");
    }

}
