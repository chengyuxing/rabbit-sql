package tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import func.FCondition;
import func.FFilter;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PGobject;
import rabbit.common.types.DataRow;
import rabbit.sql.dao.*;
import rabbit.sql.page.PagedResource;
import rabbit.sql.support.ICondition;
import rabbit.sql.support.IOutParam;
import rabbit.sql.transaction.Tx;
import rabbit.sql.dao.Args;
import rabbit.sql.types.DataFrame;
import rabbit.sql.types.OUTParamType;
import rabbit.sql.types.Param;
import rabbit.sql.utils.JdbcUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;


public class MyTest {

    private static BakiDao baki;
    private static BakiDao baki2;
    private static HikariDataSource dataSource;
    private static HikariDataSource dataSource2;

    private static final ObjectMapper json = new ObjectMapper();

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        SQLFileManager manager = new SQLFileManager("pgsql/data.sql");

        BakiDao bakiDao = BakiDao.of(dataSource);
        bakiDao.setSqlFileManager(manager);
        baki = bakiDao;
        baki2 = BakiDao.of(dataSource);
//        bakiDao.setSqlPath("pgsql");
//        baki2 = new BakiDao(dataSource2);
    }

    @Test
    public void selectTest() throws Exception {
        baki.fetch("select * from test.tb where blob is not null")
                .ifPresent(System.out::println);
    }

    @Test
    public void inserts() throws Exception {
        baki.execute("insert into test.tb(strs)\n" +
                "values ('{a,b,c}')");
    }

    @Test
    public void toEntity() throws Exception {
        baki.fetch("select * from test.tb")
                .ifPresent(row -> {
                    Tb tb = row.toEntity(Tb.class);
                    System.out.println(tb);
                    System.out.println(tb.getStrs().get(0));
                    System.out.println(tb.getDt());
                });
    }

    @Test
    public void testFieldCase() throws Exception {
        baki.query("select 1 A, 2 \"B\", current_date DT, now() \"NoW\"")
                .forEach(System.out::println);
    }

    @Test
    public void insert() throws Exception {
        DataFrame dataFrame = DataFrame.of("test.tb", Args.create()
                .add("ts", "2020年2月12日 11:22:33")
                .add("dtm", "")
                .add("tm", "23时55分13秒")
                .add("bak", "ccc"))
                .strict(false);
        baki.insert(dataFrame);
    }

    @Test
    public void inertEntity() throws Exception {

        Me me = new Me();
        me.setAge(25);
        me.setName("entity");
        DataFrame frame = DataFrame.of("test.tb", Args.of("jsb", me));
        baki.insert(frame);
    }

    @Test
    public void insertFile() throws FileNotFoundException {
        baki.insert(DataFrame.of("test.tb", Args.of("blob", new FileInputStream("/Users/chengyuxing/Downloads/istatmenus6.40.zip"))));
    }

    @Test
    public void executeAny() throws Exception {
        DataRow row = baki.execute("(select current_date, current_time)");
        System.out.println(row);
        row.<List<DataRow>>get(0)
                .forEach(System.out::println);

//        DataRow row1 = baki.execute("insert into test.tb(a,b) values (:a,:b)", Args.<Object>of("a", 5).add("b", 5));
//        System.out.println(row1);
//
//        DataRow row2 = baki.execute("create index idx_a on test.tb(a)");
//        System.out.println(row2);
    }

    @Test
    public void pagerTest() throws Exception {
        PagedResource<DataRow> res = baki.<DataRow>query("&pgsql.data.select_user", 1, 10)
                .args(Args.create().add("id", 35))
                .collect(d -> d);
        System.out.println(res);
    }

    @Test
    public void dynamicSqlTest() throws Exception {
        try (Stream<DataRow> s = baki.query("&pgsql.data.logical", Args.create()
                .add("age", 91)
                .add("name", "小"))) {
            s.forEach(System.out::println);
        }
    }

    @Test
    public void jdbcTest() throws Exception {
        System.out.println(JdbcUtil.supportsNamedParameters(dataSource.getConnection()));
    }

    @Test
    public void array() throws Exception {
        baki.fetch("select array [12,13,11,4,5,6.7]:: varchar[], '{\"a\":\"chengyuxing\"}'::jsonb")
                .ifPresent(r -> {
                    System.out.println(r);
                    System.out.println(r.getType(0));

//                        Array arr = r.get(0);
//                        Integer[] ints = (Integer[]) arr.getArray();
//                        System.out.println(Arrays.toString(ints));
                });
    }

    @Test
    public void loadData() throws Exception {
        baki.execute("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','");
//        baki.execute("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','");
    }

    @Test
    public void inserta() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("fruitname", "ccc");
        map.put("productplace", "bbb");
        map.put("price", 1000);

        int i = baki.insert(DataFrame.of("test.fruit", map));
        System.out.println(i);
    }

    @Test
    public void pageTest() throws Exception {
        baki.query("select * from test.region")
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
        baki.query("select * from test.user where id --用户ID\n" +
                " = 3;").forEach(System.out::println);
//        xDao2.query("select * from user;", DataRow::toMap, -1).forEach(System.out::println);
    }

    @Test
    public void testSqlFile() {
        try (Stream<DataRow> s = baki.query("&data.query",
                Args.create().add("id", 4))) {
            s.map(DataRow::toMap)
                    .forEach(System.out::println);
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
        baki.call("call test.transaction()", Args.create());
    }

    @Test
    public void testCall() throws Exception {
        Tx.using(() -> baki.call("{:res = call test.fun_query()}",
                Args.of("res", Param.OUT(OUTParamType.REF_CURSOR)))
                .<List<DataRow>>get(0)
                .stream()
                .map(DataRow::toMap)
                .forEach(System.out::println));
    }

    @Test
    public void testCall2() throws Exception {
        baki.call(":num = call test.get_grade(:id)",
                Args.of("num", Param.OUT(OUTParamType.INTEGER))
                        .add("id", Param.IN(5)))
                .getNullable("num")
                .ifPresent(System.out::println);
    }


    @Test
    public void multi_res_function() throws Exception {
        Tx.using(() -> {
            DataRow row = baki.call("call test.multi_res(12, :success, :res, :msg)",
                    Args.of("success", Param.OUT(OUTParamType.BOOLEAN))
                            .add("res", Param.OUT(OUTParamType.REF_CURSOR))
                            .add("msg", Param.OUT(OUTParamType.VARCHAR))
            );
            System.out.println(row);
        });
    }

    @Test
    public void parameterMeta() throws Exception {
        Connection connection = dataSource.getConnection();
        CallableStatement statement = connection.prepareCall("{call test.func_now(1,2,?,?,?)}");
        ParameterMetaData metaData = statement.getParameterMetaData();
        System.out.println(metaData.getParameterTypeName(1));
        System.out.println(metaData.getParameterTypeName(2));
        System.out.println(metaData.getParameterTypeName(3));
        System.out.println(metaData.getParameterTypeName(4));
        System.out.println(metaData.getParameterTypeName(5));
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
        DataRow row = baki.call("{call test.fun_now(:a, :b, :sum, :dt, :tm)}",
                Args.of("dt", Param.OUT(OUTParamType.TIMESTAMP))
                        .add("a", Param.IN(11))
                        .add("b", Param.IN(22))
                        .add("sum", Param.OUT(OUTParamType.INTEGER))
                        .add("tm", Param.OUT(new TIME())));
        Timestamp dt = row.get("dt");
        System.out.println(dt.toLocalDateTime());
        System.out.println(row);
        System.out.println((int) row.get("sum"));
        System.out.println((Time) row.get("tm"));
        PGobject obj = new PGobject();
    }

    @Test
    public void jackson() throws Exception {
        Class<?> clazz = Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
        Object objectMapper = clazz.newInstance();
        Method method = clazz.getDeclaredMethod("writeValueAsString", Object.class);
        Object jsonStr = method.invoke(objectMapper, Arrays.asList(1, 2, 3));

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
    public void TestDelete() throws SQLException {
        int i = baki.delete("test.user", Condition.where(Filter.like("name", "batch%")));
        System.out.println(i);
    }

    @Test
    public void testUpdate() throws SQLException {
        int i = baki.update("test.user t",
                Args.create().add("name", Param.IN("SQLFileManager")),
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
