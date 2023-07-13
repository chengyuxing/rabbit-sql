package tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.common.ImmutableList;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.IPipe;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.page.impl.OraclePageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import com.zaxxer.hikari.HikariDataSource;
import func.BeanUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nutz.json.Json;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Tests {

    @Test
    public void page() throws Exception {
        PGPageHelper pgPageHelper = new PGPageHelper();
        pgPageHelper.init(3, 10, 0);
        System.out.println(pgPageHelper);
        System.out.println(pgPageHelper.limit());
        System.out.println(pgPageHelper.offset());
        System.out.println(pgPageHelper.pagedArgs());
    }

    @Test
    public void dtTest() throws Exception {
        System.out.println(new java.sql.Time(DateTimes.toEpochMilli("2020-12-11 11:22:33")));
        System.out.println(LocalDateTime.class.getTypeName());
    }

    @Test
    public void dsTest() throws Exception {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        BakiDao bakiDao = BakiDao.of(dataSource);
    }

    @Test
    public void dtTest2() throws Exception {
        String ts = "12月11 23:12:55";
        String dt = "2020-12-11";
        String tm = "23时12分55秒";

        System.out.println(DateTimes.toDate(ts));
        System.out.println(DateTimes.now().toString("yyyy-MM-dd"));
    }

    @Test
    public void formatDt() throws Exception {
        System.out.println(DateTimes.of(new Date()).toString("yyyy年MM月dd日 HH:mm:ss"));
        for (int i = 0; i < 12; i++) {
            System.out.println(new Date(LocalDateTime.now().plusMonths(i).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()));
        }
    }

    @Test
    public void Datarows() throws Exception {

    }

    private static ImmutableList<Integer> immutableList;

    @BeforeClass
    public static void init() {
        final List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            list.add(i);
        }
        immutableList = ImmutableList.of(list);
    }

    @Test
    public void lambdaTest() throws Exception {
        immutableList.foreach(System.out::println);
    }

    @Test
    public void testMatcher() throws Exception {
        String str = "select t.id || 'number' || 'name:cyx', '{\"name\":\"user\"}' from test.user";
        String regex = "'[^']*'";
        Matcher matcher = Pattern.compile(regex, Pattern.MULTILINE).matcher(str);
        while (matcher.find()) {
            str = str.replace(matcher.group(), "${*}");
        }
        System.out.println(str);
    }

    @Test
    public void sqlReplace() throws Exception {
        String str = "select t.id || 'number' || 'age:age,name:cyx', '{\"name\":\"user\"}'::jsonb from test.user where id =:id::integer and id >:idc and name=text :username";
        String upd = "update test.score set grade =  :grade::integer" +
                " where id =   :id_˞0::integer::integer or id >   :id_˞1::integer::integer";

        String sql = "insert into test.user(idd,name,id,age,address) values (:id,:name::integer,:idd::float,integer :age,date :address)";
//        String sql2 = "select * from test.user where id = '1' and tag = '1' and num = '1' and name = :name";
//        String jsonSql = "select '{\"a\":[1,2,3],\"b\":[4,5,6]}'::json #>> '{b,1}'";
        Pair<String, List<String>> pair = new SqlGenerator(':').getPreparedSql(str, Collections.emptyMap());
        System.out.println(pair.getItem1());
        System.out.println(pair.getItem2());

        Pair<String, Map<String, String>> stringMapPair = SqlUtil.replaceSqlSubstr(str);
        System.out.println(stringMapPair.getItem1());
        System.out.println(stringMapPair.getItem2());
    }

    @Test
    public void sqlPlaceHolder() throws Exception {
        String query = "select * from test where id = ?id and id = ?id and idCard = '5301111' or name = ?name ${cnd}";
        Pair<String, List<String>> sql = new SqlGenerator('?').generateSql(query, Args.of("cnd", "and date <= '${date}'")
                .add("date", "2020-12-23 ${time}")
                .add("time", "11:23:44"), true);
        System.out.println(sql.getItem1());
        System.out.println(sql.getItem2());
    }

    @Test
    public void hash() throws Exception {
        System.out.println("null instanceof String".equals(null));
    }

    @Test
    public void regexTest() throws Exception {
        String str = "select t.id || 'number' || 'age:age,name:cyx', '{\"name\":\"user\"}'::jsonb from test.user where id =:id::integer and id >:idc and name=text:username";

        Pattern pattern = Pattern.compile("[^:]:(?<name>[\\w.]+)");
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            System.out.println(matcher.group("name"));
        }
    }

    @Test
    public void orderByTest() throws Exception {
        String a = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String b = a.toLowerCase();
        for (int i = 0; i < a.length(); i++) {
            System.out.print((int) a.charAt(i) + ", ");
        }
        System.out.println();
        for (int i = 0; i < a.length(); i++) {
            System.out.print((int) b.charAt(i) + ", ");
        }
    }

    public static void printMap(Map<String, Param> map) {
        System.out.println(map);
    }

    @Test
    public void generateSql() throws Exception {
        Args<Object> paramMap = Args.create()
                .add("a", "cyx")
                .add("b", "v")
                .add("c", "")
                .add("d", null)
                .add("e", "1");

//        String sql = SqlUtil.generateInsert("test.user", paramMap, Ignore.BLANK, Arrays.asList("c", "d", "a"));

//        String upd = new SqlGenerator(':').generateNamedParamUpdate("test.user", paramMap);
//        System.out.println(upd);
    }

    @Test
    public void recordTest() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 1);
        map.put("d", 1);
        map.put("e", 1);
        Set<String> strings = new HashSet<>(map.keySet());

        Iterator<String> iterator = strings.iterator();
        if (iterator.hasNext()) {
            String v = iterator.next();
            iterator.remove();
        }
        System.out.println(strings);
        System.out.println(map);
    }

    @Test
    public void placeholder() {
        String sql = "select *\n" +
                "from test.\"user\" --用户表\n" +
                "/*啊/*dddd*/啊啊啊*/\n" +
                "where id = :id\n" +
                "${cnd} \n" +
                "/*插入语句。*/\n";

        Pattern pattern = Pattern.compile("/\\*(?<name>.+)\\*/");
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            System.out.println(matcher.group("name"));
        }
        System.out.println(sql.replaceAll(pattern.pattern(), ""));
    }

    @Test
    public void SqlFileTest() throws IOException, URISyntaxException {
        XQLFileManager manager = new XQLFileManager();
        manager.add("pg", "pgsql/test.sql");
        manager.setConstants(Args.of("db", "test").add("fields", "name, address, enable"));
        manager.init();
        System.out.println("-------------");
    }

    @Test
    public void LambdaTest() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        String name = BeanUtil.convert2fieldName(User::getName);
        System.out.println(name);
    }

    private static <T> void lambda(Class<T> clazz, Function<T, Object> fun) throws IllegalAccessException, InstantiationException {
        T ins = clazz.newInstance();
        System.out.println(fun.apply(ins));
    }

    @Test
    public void asd() throws Exception {

    }

    @Test
    public void trimEnds() throws Exception {
        System.out.println(SqlUtil.trimEnd("where id = 10\r\n;  ;;;\t\r\n"));
    }

    @Test
    public void sql() throws Exception {
        String sql = "with recursive cte(id, name, pid) as (\n" +
                "    select id, name, pid\n" +
                "    from test.region\n" +
                "    where id = 4\n" +
                "    union all\n" +
                "    select t.id, t.name, t.pid\n" +
                "    from cte c,\n" +
                "         test.region t\n" +
                "    where t.pid = c.id\n" +
                ")\n" +
                "select *\n" +
                "from cte;";
        String cq = new SqlGenerator(':').generateCountQuery(sql);
        System.out.println(cq);
    }

    @Test
    public void pageTest() throws Exception {
        OraclePageHelper page = new OraclePageHelper();
        page.init(5, 12, 100);
        System.out.println(page.start());
        System.out.println(page.end());
        PagedResource<Integer> pagedResource = PagedResource.of(page, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        System.out.println(pagedResource);
        System.out.println(Json.toJson(pagedResource));
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writeValueAsString(pagedResource));
    }

    @Test
    public void inseertSql() throws Exception {
        DataRow row = DataRow.fromPair("id", 15,
                "name", "chengyuxing",
                "words", "it's my time!",
                "dt", LocalDateTime.now());
        System.out.println(row);

        Args<Object> args = Args.create("id", 15,
                "name", "chengyuxing",
                "words", "it's my time!",
                "dt", LocalDateTime.now());

        System.out.println(new SqlGenerator('?').generateNamedParamInsert("t.user", args, Arrays.asList("id", "name", "asx")));
        System.out.println(new SqlGenerator('?').generateInsert("t.user", args, Collections.emptyList()));

    }

    @Test
    public void testInserGenerate() {
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        String[] allFields = new String[]{"a", "b", "c", "d", "e", "f"};
        Args<Object> args = Args.create("A", 1, "B", 2, "C", 3, "D", 4);
        System.out.println(sqlGenerator.generateNamedParamInsert("user", args, Arrays.asList(allFields)));
    }

    @Test
    public void testFilter() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        map.put("A", 1);
        map.put("B", 1);
        map.put("C", 1);
        map.put("D", 1);

        SqlGenerator sqlGenerator = new SqlGenerator(':');

        System.out.println(sqlGenerator.filterKeys(map, Arrays.asList("a", "b", "c")));
        System.out.println(sqlGenerator.generateNamedParamInsert("test.t", map, Arrays.asList("a", "b", "c")));

    }

    @Test
    public void test() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("cyx", "pgsql/data.sql");
        Set<String> names = new HashSet<>();
        names.add("pgsql/dynamic.sql");
        xqlFileManager.init();

        xqlFileManager.remove("cyx");

        System.out.println(ReflectUtil.obj2Json(xqlFileManager));
    }

    @Test
    public void testf1() {
        try {
            XQLFileManager xqlFileManager = new XQLFileManager();
            xqlFileManager.add("abc", "pgsql/data.sql");
            xqlFileManager.add("pgsql/data.sql");
            xqlFileManager.init();
        } catch (DuplicateException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Object xql = ReflectUtil.getInstance(XQLFileManager.class);
        Method m = xql.getClass().getDeclaredMethod("setFilenames", Set.class);
        m.invoke(xql, Stream.of("a/v/d/bbb.xql", "111/222/ddd.xql").collect(Collectors.toSet()));
        System.out.println(ReflectUtil.obj2Json(xql));
    }

    @Test
    public void test11() {
        System.out.println(Paths.get("/Users/chengyuxing/Downloads/flatlaf-demo-3.0.jar").toUri());
    }

    @Test
    public void tesy3() {
        Path path1 = Paths.get("/Users/chengyuxing/Downloads/flatlaf-demo-3.0.jar");
        Path path2 = Paths.get("/Users/chengyuxing/Downloads/flatlaf-demo-3.0.jar");
        Set<Path> paths = new HashSet<>();
        paths.add(path1);
        paths.add(path2);
        System.out.println(paths);
    }

    @Test
    public void test111() throws IOException, URISyntaxException {
        FileResource resource = new FileResource("file:/Users/chengyuxing/Downlaaaoads/flatlaf-demo-3.0.jar");
        System.out.println(resource.exists());
        URI uri = new URI("file:/Users/chengyuxing/Downloads/flatlaf-demo-3.0.jar");
        System.out.println(Files.exists(Paths.get(uri)));
        System.out.println(uri.toURL().getFile());
    }

    @Test
    public void test2() {
        System.out.println(Paths.get("/addd/ccc/data.sql").getFileName().toString());
    }

    @Test
    public void test112() {
        Map<String, String> map1 = new LinkedHashMap<>();
        map1.put("b", "1");
        map1.put("d", "1");
        map1.put("a", "1");

        System.out.println(Collections.unmodifiableMap(map1));
    }

    @Test
    public void test3() throws IOException {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("cyx", "pgsql/data.sql");
        xqlFileManager.add("abc", "pgsql/nest.sql");
        xqlFileManager.setPipeInstances(Args.of("km2null", (IPipe<String>) value -> {
            if (Objects.equals("kunming", value)) {
                return null;
            }
            return value.toString();
        }));
        xqlFileManager.init();
//        Map<String, String> map = xqlFileManager.sqlFileStructured(new FileResource("pgsql/data.sql"));
        System.out.println(xqlFileManager);

        System.out.println(xqlFileManager.get("cyx.query", Args.create(
                "name", "cyx",
                "address", "kunming",
                "age", 103
        ), false));
    }
}
