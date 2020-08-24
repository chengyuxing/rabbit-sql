package tests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import func.FCondition;
import func.FFilter;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.common.tuple.Pair;
import rabbit.common.types.DataRow;
import rabbit.sql.dao.*;
import rabbit.sql.Light;
import rabbit.sql.support.ICondition;
import rabbit.sql.transaction.Tx;
import rabbit.sql.types.OUTParamType;
import rabbit.sql.types.Param;
import rabbit.sql.utils.JdbcUtil;
import rabbit.sql.utils.SqlUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

import static rabbit.sql.utils.SqlUtil.getPreparedSqlAndIndexedArgNames;

public class MyTest {

    private static Light light;
    private static Light light2;
    private static HikariDataSource dataSource;
    private static HikariDataSource dataSource2;

    private static final ObjectMapper json = new ObjectMapper();

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");
        dataSource.setDriverClassName("org.postgresql.Driver");

        SQLFileManager manager = new SQLFileManager("pgsql/data.sql");

        LightDao lightDao = LightDao.of(dataSource);
        lightDao.setSqlFileManager(manager);
        light = lightDao;
        light2 = LightDao.of(dataSource);
//        lightDao.setSqlPath("pgsql");
//        light2 = new LightDao(dataSource2);
    }

    @Test
    public void dynamicSqlTest() throws Exception {
        try (Stream<DataRow> s = light.query("&data.logical", ParamMap.create()
                .putIn("age", 91)
                .putIn("name", "小"))) {
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

        int i = light.insert("test.fruit", ParamMap.from(map));
        System.out.println(i);
    }

    @Test
    public void pageTest() throws Exception {
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
                ParamMap.create().putIn("id", 4))) {
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
    public void testCall() throws Exception {
        List<DataRow> rows = Tx.using(() -> light.function("call test.fun_query(:c::refcursor)",
                ParamMap.create().putInOut("c", "result", OUTParamType.REF_CURSOR))
                .get(0));
        rows.forEach(System.out::println);
//        for (int i = 0; i < 10; i++) {
//            TimeUnit.SECONDS.sleep(10);
//            System.out.println(i);
//        }
    }

    @Test
    public void Var() throws Exception {
        Pair<String, List<String>> a = SqlUtil.getPreparedSqlAndIndexedArgNames("call test.now3(:a,:b,:r,:n)");
        System.out.println(a.getItem1());
        System.out.println(a.getItem2());
    }

    @Test
    public void multi_res_function() throws Exception {
        Tx.using(() -> {
            DataRow row = light.function("call test.multi_res(12, :success, :res, :msg)",
                    ParamMap.create()
                            .putOut("success", OUTParamType.BOOLEAN)
                            .putOut("res", OUTParamType.REF_CURSOR)
                            .putOut("msg", OUTParamType.VARCHAR)
            );
            System.out.println(row);
        });
    }

    @Test
    public void callTest() throws Exception {
        DataRow row = light.function("call test.now3(101,55,:r,:n)",
                ParamMap.create()
//                        .putIn("a", 101)
//                        .putIn("b", 55)
                        .putOut("r", OUTParamType.TIMESTAMP)
                        .putOut("n", OUTParamType.INTEGER));
        Timestamp dt = row.get("r");
        System.out.println(dt.toLocalDateTime());
        System.out.println(row);
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
                int x = light.insert("test.user", ParamMap.create()
                        .putIn("name", "chengyuxing_outer_transaction")
                        .putIn("password", "1993510"));
                int y = light2.insert("test.user", ParamMap.create()
                        .putIn("name", "jackson_outer_transaction")
                        .putIn("password", "new Date("));
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
        List<Map<String, Param>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(ParamMap.create()
                    .putIn("name", "batch" + i)
                    .putIn("password", "123456"));
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
                ParamMap.create().putIn("name", Param.IN("SQLFileManager")),
                Condition.where(Filter.eq("id", 5)));
        System.out.println(i);
    }

    @Test
    public void placeholder() {
        System.out.println(getPreparedSqlAndIndexedArgNames("select * from test.user t where t.id = #{id} and t.name = #{name}"));
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

    @SuppressWarnings("unchecked")
    @Test
    public void FConditionTest() throws Exception {
        FCondition<User> f = FCondition.<User>builder().where(FFilter.eq(User::getName, "cyx"))
                .and(FFilter.eq(User::getPassword, "123456"), FFilter.eq(User::getName, "admin"))
                .or(FFilter.like(User::getName, "%admin"))
                .orderBy(User::getAge);

        System.out.println(f.getParams());
        System.out.println(f.toString());
    }

    @Test
    public void ParamsTest() {

    }
}
