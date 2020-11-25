package tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import func.BeanUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nutz.dao.entity.Record;
import org.nutz.json.Json;
import rabbit.common.tuple.Pair;
import rabbit.common.types.DataRow;
import rabbit.common.types.ImmutableList;
import rabbit.common.utils.DateTimes;
import rabbit.sql.dao.*;
import rabbit.sql.page.impl.OraclePageHelper;
import rabbit.sql.page.PagedResource;
import rabbit.sql.dao.Args;
import rabbit.sql.types.Ignore;
import rabbit.sql.types.Param;
import rabbit.sql.utils.SqlUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.time.*;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static rabbit.sql.utils.SqlUtil.SEP;

public class Tests {

    @Test
    public void dtTest() throws Exception {
        System.out.println(new java.sql.Time(DateTimes.toEpochMilli("2020-12-11 11:22:33")));
        System.out.println(LocalDateTime.class.getTypeName());
    }

    @Test
    public void dtTest2() throws Exception {
        String ts = "12月11 23:12:55";
        String dt = "2020-12-11";
        String tm = "23时12分55秒";

        System.out.println(DateTimes.toDate(ts));
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
        Map<Integer, Object> map = new HashMap<>();
        map.put(1, "a");
        map.put(2, "2");
        map.put(3, false);
        map.put(4, 10.01);

        DataRow row = DataRow.fromMap(map);

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
    public void sqlTest() throws Exception {
        String sql = "select id, filename, title\n" +
                "from test.user\n" +
                "where\n" +
                "--#if :title <> blank\n" +
                "and title = :title\n" +
                "--#fi\n" +
                "order by sj desc";
        String dq = SqlUtil.dynamicSql(sql, Args.of("title", "op"));
        System.out.println(dq);
        System.out.println(SqlUtil.generateCountQuery(dq));
    }

    @Test
    public void iteratorTest() throws Exception {
        while (immutableList.hasNext()) {
            System.out.println(immutableList.next());
        }
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

        String sql = "insert into test.user(idd,name,id,age,address) values (:id,:name::integer,:idd" + SEP + "::float,integer :age,date :address)";
//        String sql2 = "select * from test.user where id = '1' and tag = '1' and num = '1' and name = :name";
//        String jsonSql = "select '{\"a\":[1,2,3],\"b\":[4,5,6]}'::json #>> '{b,1}'";
        Pair<String, List<String>> pair = SqlUtil.getPreparedSql(sql);
        System.out.println(pair.getItem1());
        System.out.println(pair.getItem2());
    }

    @Test
    public void regex() throws Exception {
        Matcher m = SqlUtil.ARG_PATTERN.matcher(":now = call test.now()");
        while (m.find()) {
            System.out.println(m.group("name"));
        }
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

        String upd = SqlUtil.generateUpdate("test.user", paramMap);
        System.out.println(upd);
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
        SQLFileManager manager = new SQLFileManager("pgsql");
        manager.init();
        System.out.println("----");
        System.out.println(manager.get("data.query"));
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
        String cq = SqlUtil.generateCountQuery(sql);
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
}
