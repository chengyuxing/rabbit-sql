package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.ImmutableList;
import com.github.chengyuxing.common.MostDateTime;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
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
    public void testHash() {
        System.out.println(StringUtil.hash("","MD5"));
    }

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
        System.out.println(new java.sql.Time(MostDateTime.toEpochMilli("2020-12-11 11:22:33")));
        System.out.println(LocalDateTime.class.getTypeName());
    }

    @Test
    public void dsTest() throws Exception {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        BakiDao bakiDao = new BakiDao(dataSource);
    }

    @Test
    public void dtTest2() throws Exception {
        String ts = "12月11 23:12:55";
        String dt = "2020-12-11";
        String tm = "23时12分55秒";

        System.out.println(MostDateTime.toDate(ts));
        System.out.println(MostDateTime.now().toString("yyyy-MM-dd"));
    }

    @Test
    public void formatDt() throws Exception {
        System.out.println(MostDateTime.of(new Date()).toString("yyyy年MM月dd日 HH:mm:ss"));
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
        String sql = "insert into test.user(idd,name,id,age,address) values (:id,:name::integer,:idd::float,integer :age,date :address)";
        SqlGenerator.GeneratedSqlMetaData pair = new SqlGenerator(':').generatePreparedSql(str, Collections.emptyMap());
        System.out.println(pair.getArgs());
        System.out.println(pair.getResultSql());

        Pair<String, Map<String, String>> stringMapPair = SqlUtil.escapeSubstring(str);
        System.out.println(stringMapPair.getItem1());
        System.out.println(stringMapPair.getItem2());
    }

    @Test
    public void testSql() {
        String sql = "select kb.ajbh,aj.ajmc,aj.ajlxmc,aj.ajlbmc,kb.lasj,\n" +
                "       decode(qbdq, '5004', '取保到期', '') || ',' || decode(jjdq, '监居到期', '') || ',' ||\n" +
                "       decode(wxyr, '5001', '无嫌疑人', '') || ',' || decode(wqzcs, '5002', '嫌疑人无强制措施', '') ||\n" +
                "       ',' ||decode(qzcsdq, '5003', '强制措施到期')                              as wpajbz,\n" +
                "       decode(zj.ajscjb,'1','一级审查','2','二级审查') as ajscjb,\n" +
                "       decode(zj.clzt,'0','未处理','1','办案单位已处理') as clzt,\n" +
                "       zbyj.yjnr,zbyj.lrdwdm,zbyj.lrdwmc,zbyj.lrsj,qs.lrsj as qssj,fk.clnr,\n" +
                "       decode(fk.cljg,'1','完成','2','部分完成','3','未完成') as cljg,\n" +
                "       fk.lrsj as fksj\n" +
                "from zhag_xs_wpajxckb kb\n" +
                "         join g2bajd.ZFBA_GT_ajzbyjxx zbyj on kb.ajbh=zbyj.ajbh and zbyj.jlbz='1'\n" +
                "         left join g2bajd.ZFBA_GT_ajzbyjqsxx qs on qs.yjbh = zbyj.jlbh and qs.jlbz='1'\n" +
                "         left join g2bajd.ZFBA_GT_ajzbyjfkxx fk on fk.yjbh = zbyj.jlbh and fk.jlbz='1'\n" +
                "         join g2bajd.ZFBA_GT_WPXSAJZJSCB zj on zj.ajbh=kb.ajbh and zj.jlbz='1'\n" +
                "         join V_ZHAG_XSAJXX aj on kb.ajbh=aj.ajbh\n" +
                "where zbyj.lrsj <= to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')\n" +
                "  and zbyj.lrsj >= to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss')\n" +
                "  -- #if :dwdm != blank\n" +
                "  and zbyj.lrdwdm in(select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))\n" +
                "  -- #fi\n" +
                "  order by kb.ajbh,zbyj.lrsj";
        SqlGenerator generator = new SqlGenerator(':');
        System.out.println(generator.generatePreparedSql(sql, Collections.emptyMap()));
        System.out.println(generator.getNamedParamPattern());
        Matcher m = generator.getNamedParamPattern().matcher(sql);
        while (m.find()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                System.out.println("Group " + i + ": " + m.group(i));
            }
        }
    }

    @Test
    public void sqlPlaceHolder() throws Exception {
        String query = "select * from test where id = ?_i.d and id = ?id and idCard = '5301111' or name = ?na_me ${cnd}";
        SqlGenerator.GeneratedSqlMetaData sql = new SqlGenerator('?').generatePreparedSql(query, DataRow.of("cnd", "and date <= '${date}'")
                .add("date", "2020-12-23 ${time}")
                .add("time", "11:23:44"));
        System.out.println(sql.getArgs());
        System.out.println(sql.getResultSql());
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
        page.init(1, 10, 100);
        System.out.println(page.start());
        System.out.println(page.end());
        System.out.println(page.pagedSql("select * from test.user"));
//        PagedResource<Integer> pagedResource = PagedResource.of(page, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
//        System.out.println(pagedResource);
//        System.out.println(Json.toJson(pagedResource));
//        ObjectMapper mapper = new ObjectMapper();
//        System.out.println(mapper.writeValueAsString(pagedResource));
    }

    @Test
    public void testPage2() {
        PGPageHelper pageHelper = new PGPageHelper();
        pageHelper.init(5, 10, 150);
        System.out.println(pageHelper);
    }

    @Test
    public void testFilter() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        map.put("A", 1);
        map.put("B", null);
        map.put("C", 1);
        map.put("D", 1);

        SqlGenerator sqlGenerator = new SqlGenerator(':');

        System.out.println(sqlGenerator.filterKeys(map, Arrays.asList("a", "b", "c")));
        System.out.println(sqlGenerator.generateNamedParamInsert("test.t", map, Arrays.asList("a", "b", "c"), true));

    }

    @Test
    public void test() {
        StringJoiner stringJoiner = new StringJoiner(" ");
        stringJoiner.setEmptyValue("--");
        stringJoiner.add("");
        stringJoiner.add("");
        stringJoiner.add("");
        stringJoiner.add("");
        stringJoiner.add("");
        String res = stringJoiner.toString();
        System.out.println(res);
    }

    @Test
    public void testf1() {
        try {
            XQLFileManager xqlFileManager = new XQLFileManager();
            xqlFileManager.add("pgsql/data.sql");
            xqlFileManager.init();
            System.out.println("------------");
            System.out.println("------------");
            System.out.println(xqlFileManager.get("data.update", Args.of(
                    "name", "cyx",
                    "age", 31,
                    "open", "abc",
                    "address", "kunaming"
            )));

        } catch (DuplicateException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Object xql = ReflectUtil.getInstance(XQLFileManager.class);
        Method m = xql.getClass().getDeclaredMethod("setFilenames", Set.class);
        m.invoke(xql, Stream.of("a/v/d/bbb.xql", "111/222/ddd.xql").collect(Collectors.toSet()));
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
}
