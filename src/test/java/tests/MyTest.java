package tests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import func.FCondition;
import func.FFilter;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.common.types.DataRow;
import rabbit.sql.dao.*;
import rabbit.sql.page.PagedResource;
import rabbit.sql.support.ICondition;
import rabbit.sql.support.IOutParam;
import rabbit.sql.transaction.Tx;
import rabbit.sql.types.OUTParamType;
import rabbit.sql.types.Param;
import rabbit.sql.utils.JdbcUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;


public class MyTest {

    private static LightDao light;
    private static LightDao light2;
    private static HikariDataSource dataSource;
    private static HikariDataSource dataSource2;

    private static final ObjectMapper json = new ObjectMapper();

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        SQLFileManager manager = new SQLFileManager("pgsql/data.sql");

        LightDao lightDao = LightDao.of(dataSource);
        lightDao.setSqlFileManager(manager);
        light = lightDao;
        light2 = LightDao.of(dataSource);
//        lightDao.setSqlPath("pgsql");
//        light2 = new LightDao(dataSource2);
    }

    @Test
    public void executeAny() throws Exception {
        DataRow row = light.execute("(select current_date, current_time)");
        System.out.println(row);
        row.<List<DataRow>>get(0)
                .forEach(System.out::println);

//        DataRow row1 = light.execute("insert into test.tb(a,b) values (:a,:b)", Args.<Object>of("a", 5).set("b", 5));
//        System.out.println(row1);
//
//        DataRow row2 = light.execute("create index idx_a on test.tb(a)");
//        System.out.println(row2);
    }

    @Test
    public void pagerTest() throws Exception {
        PagedResource<DataRow> res = light.<DataRow>query("&pgsql.data.select_user", 1, 10)
                .args(Args.create().set("id", 35))
                .collect(d -> d);
        System.out.println(res);
    }

    @Test
    public void dynamicSqlTest() throws Exception {
        try (Stream<DataRow> s = light.query("&pgsql.data.logical", Args.create()
                .set("age", 91)
                .set("name", "小"))) {
            s.forEach(System.out::println);
        }
    }

    @Test
    public void jdbcTest() throws Exception {
        System.out.println(JdbcUtil.supportsNamedParameters(dataSource.getConnection()));
    }

    @Test
    public void array() throws Exception {
        light.fetch("select array [12,13,11,4,5,6.7]:: integer[]")
                .ifPresent(r -> {
                    try {
                        System.out.println(r);
                        System.out.println(json.writeValueAsString(r.toMap()));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

//                        Array arr = r.get(0);
//                        Integer[] ints = (Integer[]) arr.getArray();
//                        System.out.println(Arrays.toString(ints));
                });
    }

    @Test
    public void loadData() throws Exception {
        light.execute("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','");
//        light.execute("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','");
    }

    @Test
    public void inserta() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("fruitname", "ccc");
        map.put("productplace", "bbb");
        map.put("price", 1000);

        int i = light.insert("test.fruit", map);
        System.out.println(i);
    }

    @Test
    public void pageTest() throws Exception {
        light.query("select * from test.region")
                .skip(5)
                .limit(10)
                .map(r -> String.join("<>", r.getNames()))
                .forEach(System.out::println);
    }

    @Test
    public void manager() throws Exception {
        SQLFileManager manager = new SQLFileManager("pgsql");
        manager.init();
    }

    @Test
    public void oneConnectionTest() throws Exception {
        String insert = "insert into test.user(name, password) VALUES ('bbc','123456')";
        String query = "select * from test.user";
        String procedure = "{call test.fun_query(:c::refcursor)}";

        Connection connection = dataSource.getConnection();

    }

    @Test
    public void TestMultiDs() throws SQLException {
        light.query("select * from test.user where id --用户ID\n" +
                " = 3;").forEach(System.out::println);
//        xDao2.query("select * from user;", DataRow::toMap, -1).forEach(System.out::println);
    }

    @Test
    public void testSqlFile() {
        try (Stream<DataRow> s = light.query("&data.query",
                Args.create().set("id", 4))) {
            s.map(DataRow::toMap).forEach(System.out::println);
        }
    }

    @Test
    public void DataRowTest() {
        String[] fields = new String[]{"name", "age", "address", "type"};
        Object[] values = new Object[]{"Chengyuxing", 26, "云南省昆明市", "man"};
//        DataRow row = new DataRow(fields, values);
//        row.getValues().forEach(System.out::println);
//        System.out.println(row.toMap());
    }

    @Test
    public void jsonSyntax() throws Exception {
        String json = "{\n" +
                "  \"name\": \"json\",\n" +
                "  \"age\": 21\n" +
                "}";
    }

    @Test
    public void testProcedure() throws Exception {
        light.call("call test.transaction()", Args.create());
    }

    @Test
    public void testCall() throws Exception {
        Tx.using(() -> light.call("{:res = call test.fun_query()}",
                Args.of("res", Param.OUT(OUTParamType.REF_CURSOR)))
                .<List<DataRow>>get(0)
                .stream()
                .map(DataRow::toMap)
                .forEach(System.out::println));
    }

    @Test
    public void testCall2() throws Exception {
        light.call(":num = call test.get_grade(:id)",
                Args.of("num", Param.OUT(OUTParamType.INTEGER))
                        .set("id", Param.IN(5)))
                .getNullable("num")
                .ifPresent(System.out::println);
    }


    @Test
    public void multi_res_function() throws Exception {
        Tx.using(() -> {
            DataRow row = light.call("call test.multi_res(12, :success, :res, :msg)",
                    Args.of("success", Param.OUT(OUTParamType.BOOLEAN))
                            .set("res", Param.OUT(OUTParamType.REF_CURSOR))
                            .set("msg", Param.OUT(OUTParamType.VARCHAR))
            );
            System.out.println(row);
        });
    }

    @Test
    public void callTest() throws Exception {
        class TIME implements IOutParam {

            @Override
            public int getTypeNumber() {
                return Types.TIME;
            }

            @Override
            public String getName() {
                return "time";
            }
        }
        DataRow row = light.call("{call test.fun_now(101, 55, :sum, :dt, :tm)}",
                Args.of("dt", Param.OUT(OUTParamType.TIMESTAMP))
                        .set("sum", Param.OUT(OUTParamType.INTEGER))
                        .set("tm", Param.OUT(new TIME())));
        Timestamp dt = row.get("dt");
        System.out.println(dt.toLocalDateTime());
        System.out.println(row);
        System.out.println((int) row.get("sum"));
        System.out.println((Time) row.get("tm"));
    }

//    @Test
//    public void testInsert() throws SQLException {
//        int i = xDao.execute("insert into test.user (name,password) values (#{name},#{password})",
//                Param.builder()
//                        .put("name", Param.IN("XDao"))
//                        .put("password", Param.IN("123456"))
//                        .build());
//        System.out.println(i);
//    }

    @Test
    public void TestInert2() throws SQLException {
//        Transaction.begin();
        try {
            int i = Tx.using(() -> {
                int x = light.insert("test.user", Args.create().
                        set("name", "chengyuxing_outer_transaction")
                        .set("password", "1993510"));
                int y = light2.insert("test.user", Args.create()
                        .set("name", "jackson_outer_transaction")
                        .set("password", "new Date("));
                return x + y;
            });
            System.out.println(i);
//            System.out.println(x + y);
//            Transaction.commit();
        } catch (Exception e) {
//            Transaction.rollback();
            e.printStackTrace();
        }
    }

    @Test
    public void testInsertBatch() throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(Args.create()
                    .set("name", "batch" + i)
                    .set("password", "123456"));
        }
        int i = light.insert("test.user", list);
        System.out.println(i);
    }

    @Test
    public void TestDelete() throws SQLException {
        int i = light.delete("test.user", Condition.where(Filter.like("name", "batch%")));
        System.out.println(i);
    }

    @Test
    public void testUpdate() throws SQLException {
        int i = light.update("test.user t",
                Args.create().set("name", Param.IN("SQLFileManager")),
                Condition.where(Filter.eq("id", 5)));
        System.out.println(i);
    }

    @Test
    public void ArraysTest() {
        String[] a = new String[]{"a", "b", "c"};
        String[] b = new String[4];
        System.arraycopy(a, 0, b, 1, 3);
        System.out.println(Arrays.toString(b));
    }

    @Test
    public void ConditionTest() {
        ICondition conditions = Condition.where(Filter.eq("id", 25))
                .and(Filter.isNotNull("name"))
                .or(Filter.eq("id", 88))
                .or(Filter.notLike("name", "%admin"));

        System.out.println(conditions);
    }

    @Test
    public void FConditionTest() throws Exception {
        FCondition<User> f = FCondition.where(FFilter.eq(User::getName, "cyx"))
                .and(FFilter.eq(User::getPassword, "123456"))
                .or(FFilter.like(User::getName, "%admin"));

        System.out.println(f.getArgs());
        System.out.println(f.getSql());
    }

    @Test
    public void ArgsTest() {

    }
}
