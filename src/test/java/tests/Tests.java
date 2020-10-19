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
import rabbit.common.types.UncheckedCloseable;
import rabbit.sql.dao.*;
import rabbit.sql.support.ICondition;
import rabbit.sql.page.PageHelper;
import rabbit.sql.page.impl.OraclePageHelper;
import rabbit.sql.page.PagedResource;
import rabbit.sql.types.Ignore;
import rabbit.sql.types.Param;
import rabbit.sql.utils.SqlUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static rabbit.sql.utils.SqlUtil.SEP;

public class Tests {

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
    public void sqlTest() throws Exception{
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
        Pair<String, List<String>> pair = SqlUtil.getPreparedSql(str);
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
    }

    public static void printMap(Map<String, Param> map) {
        System.out.println(map);
    }

    @Test
    public void Condition2() throws Exception {
        ICondition condition = Condition.where(Filter.eq("t.id", Wrap.wrapEnd("7", "::integer")))
                .or(Filter.gt("id", Wrap.wrapEnd("100", "::integer")));
        System.out.println(condition.getSql());
        System.out.println(condition.getArgs());

        Condition condition1 = Condition.where(Filter.eq("id", Wrap.wrapEnd("7", "::integer")));
        System.out.println(condition1.getSql());
    }

    @Test
    public void generateSql() throws Exception {
        Args<Object> paramMap = Args.create()
                .set("a", null)
                .set("b", "v")
                .set("c", "")
                .set("d", null)
                .set("e", "1");

        System.out.println(SqlUtil.generateInsert("test.user", paramMap, Ignore.NULL));
    }

    @Test
    public void recordTest() throws Exception {
        Record record = new Record();
        record.put("name", "cyx");
        System.out.println(record.getString("age"));
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
