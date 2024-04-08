package tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.Jackson;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.sql.*;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.support.IOutParam;
import com.github.chengyuxing.sql.transaction.Tx;
import com.github.chengyuxing.sql.types.OUTParamType;
import com.github.chengyuxing.sql.types.Param;
import com.zaxxer.hikari.HikariDataSource;
import func.FCondition;
import func.FFilter;
import oracle.jdbc.OracleTypes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PGobject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;
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

        XQLFileManager manager = new XQLFileManager();
        manager.add("data", "pgsql/data.sql");

        BakiDao bakiDao = new BakiDao(dataSource);
        bakiDao.setXqlFileManager(manager);
        baki = bakiDao;
        baki2 = new BakiDao(dataSource);
//        bakiDao.setSqlPath("pgsql");
//        baki2 = new BakiDao(dataSource2);
    }

    @Test
    public void test22() throws Exception {
//        UpdateExecutor executor = new UpdateExecutor("test.tb", "id = :id")
//                .safeArgs();
//        executor.update(Args.create());
    }

    @Test
    public void bakiTestQuery() throws Exception {
        int i = 0;
        while (true) {
            if (i == 1) {
                try {
                    Map<String, Object> res = baki.query("select * from test.region where id > :id")
                            .args(DataRow.of("id", 10))
                            .findFirstRow();
                    System.out.println(res);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            i++;
        }
    }

    @SuppressWarnings("unchecked")
    public void mergeTreeInverse(List<Map<String, Object>> list, List<Map<String, Object>> tree) {
        System.out.println("xxxx");
        if (list.isEmpty()) {
            return;
        }
        for (int i = tree.size() - 1, j = i; i >= 0; i--) {
            Iterator<Map<String, Object>> iterator = list.iterator();
            while (iterator.hasNext()) {
                Map<String, Object> last = tree.get(i);
                Map<String, Object> first = tree.get(j - i);
                Map<String, Object> next = iterator.next();
                if (first.get("id").equals(next.get("pid"))) {
                    next.put("children", new ArrayList<>());
                    ((List<Map<String, Object>>) first.get("children")).add(next);
                    iterator.remove();
                } else if (first != last && last.get("id").equals(next.get("pid"))) {
                    next.put("children", new ArrayList<>());
                    ((List<Map<String, Object>>) last.get("children")).add(next);
                    iterator.remove();
                }
            }
            mergeTreeInverse(list, (List<Map<String, Object>>) tree.get(i).get("children"));
        }
    }

    @SuppressWarnings("unchecked")
    public void mergeTree(List<Map<String, Object>> list, Map<String, Object> tree) {
        Iterator<Map<String, Object>> iterator = list.iterator();
        List<Map<String, Object>> children = (List<Map<String, Object>>) tree.get("children");
        if (children == null) {
            children = new ArrayList<>();
            tree.put("children", children);
        }
        while (iterator.hasNext()) {
            Map<String, Object> next = iterator.next();
            if (tree.get("id").equals(next.get("pid"))) {
                next.put("children", new ArrayList<>());
                children.add(next);
                iterator.remove();
            }
        }
        for (Map<String, Object> child : children) {
            if (list.isEmpty()) {
                break;
            }
            mergeTree(list, child);
        }
    }

    @Test
    public void tree() throws Exception {
        List<Map<String, Object>> list = baki.query("select id,name,pid from test.region where pid != 1000").maps();
        Map<String, Object> tree = new HashMap<>();
        tree.put("id", 0);
        tree.put("pid", -1);
        tree.put("name", "地球");
        tree.put("children", new ArrayList<>());
        List<Map<String, Object>> trees = new ArrayList<>();
        trees.add(tree);
        mergeTreeInverse(list, trees);
        System.out.println(Jackson.toJson(trees));
        System.out.println(list.size());
    }

    @Test
    public void toEntity() throws Exception {
        baki.query("select * from test.tb")
                .findFirst()
                .ifPresent(row -> {
                    Tb tb = (Tb) row.toEntity(Tb.class);
                    System.out.println(tb);
//                    System.out.println(tb.getStrs().get(0));
                    System.out.println(tb.getDt());
                });
    }

    @Test
    public void testFieldCase() throws Exception {
        baki.query("select 1 A, 2 \"B\", current_date DT, now() \"NoW\"")
                .stream()
                .forEach(System.out::println);
    }

    @Test
    public void insert() throws Exception {
        DataRow args = DataRow.of()
                .add("ts", "2020年2月12日 11:22:33")
                .add("dtm", "")
                .add("tm", "23时55分13秒")
                .add("strs", Arrays.asList("1", "2", "3", "4"))
                .add("bak", "ccc");
        baki.insert("test.tb").safe().save(args);
        baki.insert("test.tb")
                .fast()
                .safe()
                .save(DataRow.of("ts", "2022-12-23 11:22:23", "tm", new Date(), "aaa", "bbb"));
    }

    @Test
    public void inertEntity() throws Exception {

        Me me = new Me();
        me.setAge(25);
        me.setName("entity");
        baki.insert("test.tb").save(DataRow.of("jsb", me));
    }

    @Test
    public void insertFile() throws IOException {
        baki.insert("test.tb").save(DataRow.of("blob", Files.newInputStream(Paths.get("/Users/chengyuxing/Downloads/Bob.app.zip"))));
    }


    @Test
    public void defaultPager() throws Exception {
        PagedResource<DataRow> pagedResource = baki.query("select * from test.region")
                .pageable(1, 5)
                .count(10)
                .collect(d -> d);
        System.out.println(pagedResource);
    }

    @Test
    public void updateMore() throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(DataRow.of("id", 23, "name", "昆明西山万达广场"));
        list.add(DataRow.of("id", 24, "name", "南亚风情园"));
        int i = baki.update("test.region", "id = :id").save(list);
        System.out.println(i);
    }

    @Test
    public void query() throws Exception {
        try (Stream<DataRow> s = baki.query("select ${fields} from test.region where id = :id")
                .args(DataRow.of("fields", Arrays.asList("name", "pid"), "id", 11))
                .stream()) {
            s.forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void pagerTest() throws Exception {
        PagedResource<DataRow> res = baki.query("&data.custom_paged")
                .pageable(1, 7)
                .args(DataRow.of("id", 8))
                .disableDefaultPageSql("select count(*) from test.region where id > :id")
//                .pageHelper(new MysqlPageHelper(){
//                    @Override
//                    public String pagedSql(String sql) {
//                        return super.pagedSql(sql);
//                    }
//                })
                .collect(d -> d);
        res.getData().forEach(System.out::println);
    }

    @Test
    public void dynamicSqlTest() throws Exception {
        try (Stream<DataRow> s = baki.query("&pgsql.data.logical")
                .args(DataRow.of().add("age", 91).add("name", "小"))
                .stream()) {
            s.forEach(System.out::println);
        }
    }

    @Test
    public void line() throws Exception {
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
    public void array() throws Exception {
        baki.query("select array [12,13,11,4,5,6.7]:: varchar[], '{\"a\":\"chengyuxing\"}'::jsonb")
                .findFirst()
                .ifPresent(r -> {
                    System.out.println(r);
                    System.out.println(r.getFirst());

//                        Array arr = r.get(0);
//                        Integer[] ints = (Integer[]) arr.getArray();
//                        System.out.println(Arrays.toString(ints));
                });
    }

    @Test
    public void ssss() throws Exception {
//        String[] x = Keywords.byJdbc(baki.getMetaData());
//        System.out.println(x.length);
        System.out.println(Keywords.STANDARD.length);
    }

    @Test
    public void q() throws Exception {
//        List<DataRow> rows = Arrays.asList(
//                DataRow.fromPair("words", "aaaaa", "userid", UUID.randomUUID()),
//                DataRow.fromPair("words", "dd", "userid", UUID.randomUUID()),
//                DataRow.fromPair("words", "ccc", "userid", UUID.randomUUID()),
//                DataRow.fromPair("words", "aaadaaa", "userid", UUID.randomUUID()),
//                DataRow.fromPair("words", "dddddd", "userid", UUID.randomUUID())
//        );
//        baki.fastInsert(DataFrame.ofRows("test.history", rows));
        try {
            DataRow row = baki.query("select * from test.history where id = alskdjc.sksj")
                    .findFirst()
                    .orElse(DataRow.of());
            System.out.println(row);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void loadData() throws Exception {
        baki.of("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','");
//        baki.execute("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','");
    }

    @Test
    public void inserta() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("fruitname", "ccc");
        map.put("productplace", "bbb");
        map.put("price", 1000);

        int i = baki.insert("test.fruit").save(map);
        System.out.println(i);
    }

    @Test
    public void pageTest() throws Exception {
        long c = baki.query("select * from test.region where id = 100").stream().count();
        System.out.println(c);
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
                " = 3;").rows().forEach(System.out::println);
//        xDao2.query("select * from user;", DataRow::toMap, -1).forEach(System.out::println);
    }


    @Test
    public void testX() throws Exception {
//        Tx.begin();
        try (Stream<DataRow> s = baki.query("select now()").stream().distinct()) {
            s.forEach(System.out::println);
        }
        while (true) {

        }
    }

    @Test
    public void testSqlFile() throws InterruptedException {
        try (Stream<DataRow> s = baki.query("select * from current_date,now()")
                .args(DataRow.of().add("id", 4)).stream()) {
            s.forEach(System.out::println);
        }
        try (Stream<DataRow> s = baki.query("select * from test.message --用户ID\n" +
                ";").stream()) {
            s.forEach(System.out::println);
        }

        System.out.println(baki.query("select * from current_date,now()"));

        TimeUnit.MINUTES.sleep(5);
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
    public void arg4block() throws Exception {
//        baki.execute("do\n" +
//                "$$\n" +
//                "    begin\n" +
//                "        insert into test.message (words, dt) values (?, current_timestamp);\n" +
//                "        if 13 / 1 > 1 then\n" +
//                "            insert into test.message (words, dt) values (?, current_timestamp);\n" +
//                "        end if;\n" +
//                "    end;\n" +
//                "$$;", sc -> {
//            sc.setObject(1,"bbb");
//            sc.setObject(2, "ggg");
//            sc.execute();
//            return true;
//        });
//        baki.execute("insert into test.message (words, dt) values (:wordc, current_timestamp);",
//                Args.<Object>of("wordc", "xxx").add("wordd", "yyy"));
    }

    @Test
    public void testProcedure() throws Exception {
//        baki.call("call test.transaction_rollback()", Args.create());
    }

    @Test
    public void testCallFunc() throws Exception {
//        int res = (int) baki.call("{:res = call test.slow_query(:a,:b)}",
//                        Args.of("RES", Param.OUT(OUTParamType.INTEGER))
//                                .add("a", Param.IN(13))
//                                .add("b", Param.IN(192)))
//                .getFirst();
//        System.out.println(res);
    }

    @Test
    public void testCall() throws Exception {
//        Tx.using(() -> baki.call("{:res = call test.fun_query()}",
//                        Args.of("res", Param.OUT(OUTParamType.REF_CURSOR)))
//                .<List<DataRow>>getFirstAs()
//                .forEach(System.out::println));
    }

    @Test
    public void testCall2() throws Exception {
//        baki.call(":num = call test.get_grade(:id)",
//                        Args.of("num", Param.OUT(OUTParamType.INTEGER))
//                                .add("id", Param.IN(5)))
//                .getOptional("num")
//                .ifPresent(System.out::println);
    }

    @Test
    public void testCall3() throws Exception {
//        DataRow row = baki.call("{:res = call test.mvn_dependency_query(:keywords)}",
//                Args.of("res", Param.OUT(OUTParamType.OTHER))
//                        .add("keywords", Param.IN("chengyuxing")));
//        System.out.println(row);
    }

    @Test
    public void queryInTransaction() throws Exception {
        Tx.using(() -> {
            baki.query("select current_timestamp, version()")
                    .findFirst()
                    .ifPresent(System.out::println);
            baki.insert("test.history").save(DataRow.of("userid", UUID.randomUUID(), "words", "transactional"));
        });
    }

    @Test
    public void using() throws Exception {
        try {
            baki.using(connection -> {
                try {
                    PreparedStatement statement = connection.prepareStatement("select current_timestamp, version()");
                    ResultSet resultSet = statement.executeQuery();
                    while (resultSet.next()) {
                        System.out.println(resultSet.getObject(1));
                    }
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                return 1;
            });
        } catch (ConnectionStatusException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void multi_res_function() throws Exception {
        Tx.using(() -> {
            Map<String, Param> paramMap = new HashMap<>();
            paramMap.put("success", Param.OUT(OUTParamType.BOOLEAN));
            paramMap.put("res", Param.OUT(OUTParamType.REF_CURSOR));
            paramMap.put("msg", Param.OUT(OUTParamType.VARCHAR));
            DataRow row = baki.of("call test.multi_res(12, :success, :res, :msg)")
                    .call(paramMap);
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
            public int typeNumber() {
                return Types.TIME;
            }

            public String getName() {
                return "time";
            }
        }
        Map<String, Param> params = new HashMap<>();
        params.put("dt", Param.OUT(OUTParamType.TIMESTAMP));
        params.put("a", Param.IN(11));
        params.put("b", Param.IN(22));
        params.put("sum", Param.OUT(OUTParamType.INTEGER));
        params.put("tm", Param.OUT(new TIME()));
        DataRow row = baki.of("{call test.fun_now(:a, :b, :sum, :dt, :tm)}").call(params);
        Timestamp dt = row.getAs("dt");
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
//        int i = baki.delete("test.history", Condition.where(Filter.eq("userid", "13766f06-119d-463d-9a94-8350ea172c87")));
//        System.out.println(i);
    }

    @Test
    public void testUpdate() throws SQLException {
//        int i = baki.update("test.user t",
//                Args.create().add("name", Param.IN("SQLFileManager")),
//                Condition.where(Filter.eq("id", 5)));
//        System.out.println(i);
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
//        ICondition conditions = Condition.where(Filter.eq("id", 25))
//                .and(Filter.isNotNull("name"))
//                .or(Filter.eq("id", 88))
//                .or(Filter.notLike("name", "%admin"));
//
//        System.out.println(conditions);
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
