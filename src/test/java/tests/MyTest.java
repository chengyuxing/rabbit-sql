package tests;

import com.zaxxer.hikari.HikariDataSource;
import func.FCondition;
import func.FFilter;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nutz.json.Json;
import rabbit.common.types.DataRow;
import rabbit.sql.dao.Condition;
import rabbit.sql.dao.Filter;
import rabbit.sql.dao.LightDao;
import rabbit.sql.Light;
import rabbit.sql.support.ICondition;
import rabbit.sql.support.SQLFileManager;
import rabbit.sql.page.Pageable;
import rabbit.sql.page.impl.PGPageHelper;
import rabbit.sql.transaction.Tx;
import rabbit.sql.types.OUTParamType;
import rabbit.sql.types.Order;
import rabbit.sql.types.Param;
import rabbit.sql.dao.Params;

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
    public void array() throws Exception {
        light.fetch("select array [1,2,3.3,5.67,8]:: integer[]", r -> r)
                .ifPresent(r -> {

                    Array arr = r.get(0);
                    try {
                        Integer[] ints = (Integer[]) arr.getArray();
                        System.out.println(Arrays.toString(ints));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
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

        int i = light.insert("test.fruit", Params.from(map));
        System.out.println(i);
    }

    @Test
    public void pageTest() throws Exception {
        for (int i = 0; i < 5; i++) {
            Pageable<Map<String, Object>> res = light.query("&data.select_all",
                    DataRow::toMap,
                    PGPageHelper.of(1, 10));
//            System.out.println(res);
            System.out.println(Json.toJson(Json.toJson(res)));
//            TimeUnit.SECONDS.sleep(10);
        }
    }

    @Test
    public void pageTest2() {
        Pageable<Map<String, Object>> pageable = light.query(
                "&data.fruit",
//                "select count(1) from test.student t",
                DataRow::toMap,
//                Params.empty(),
                Condition.where(Filter.lt("age", 27)),
                PGPageHelper.of(1, 10));
        System.out.println(pageable);
    }

    @Test
    public void manager() throws Exception {
        SQLFileManager manager = new SQLFileManager("pgsql");
        manager.init();
        manager.look();
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
                " = 3;", r -> r, -1).forEach(System.out::println);
//        xDao2.query("select * from user;", DataRow::toMap, -1).forEach(System.out::println);
    }

    @Test
    public void testSqlFile() {

        light.query("&data.query", r -> r, Params.builder()
                .put("id", Param.IN(3))
//                .put("cnd", Param.TEMPLATE("order by id"))
                .build())
                .forEach(System.out::println);

        light.query("select * from test.user where id --用户ID\n" +
                " = 3;", r -> r, -1).forEach(System.out::println);
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
    public void testQuery() throws SQLException {
        light.query("select * from test.user", row -> row,
                Condition.where(Filter.eq("password", "123456"))
                        .and(Filter.gtEq("id", 4))
                        .orderBy("id", Order.ASC))
                .forEach(System.out::println);
    }

    @Test
    public void testCall() throws Exception {
        Stream<DataRow> rows = Tx.using(() -> {
            DataRow row = light.call("call test.fun_query(:c::refcursor)",
                    Params.builder()
                            .put("c", Param.IN_OUT("result", OUTParamType.REF_CURSOR))
                            .build());
            System.out.println(row);
            return row.get(0);
        });
        rows.forEach(System.out::println);
//        for (int i = 0; i < 10; i++) {
//            TimeUnit.SECONDS.sleep(10);
//            System.out.println(i);
//        }
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
                int x = light.insert("test.user", Params.builder()
                        .put("name", Param.IN("chengyuxing_outer_transaction"))
                        .put("password", Param.IN("1993510"))
                        .build());
                int y = light2.insert("test.user", Params.builder()
                        .put("name", Param.IN("jackson_outer_transaction"))
                        .put("password", Param.IN("new Date("))
                        .build());
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
            list.add(Params.builder()
                    .put("name", Param.IN("batch" + i))
                    .put("password", Param.IN("123456"))
                    .build());
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
                Params.builder().put("name", Param.IN("SQLFileManager")).build(),
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
                .and(Filter.eq("password", "123456"),
                        Filter.eq("name", "admin"),
                        Filter.gtEq("id", 2),
                        Filter.isNull("name"))
                .and(Filter.isNotNull("name"))
                .or(Filter.eq("id", 88))
                .or(Filter.notLike("name", "%admin"))
                .orderBy("id", Order.DESC)
                .orderBy("name", Order.ASC);

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
        Map<String, Param> map = Params.builder()
                .put("name", Param.IN("cyx"))
                .put("age", Param.IN(26))
                .put("address", Param.IN("昆明市"))
                .build();
        System.out.println(map);
    }
}
