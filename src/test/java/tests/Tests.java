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
import rabbit.common.utils.ResourceUtil;
import rabbit.sql.dao.*;
import rabbit.sql.support.ICondition;
import rabbit.sql.page.PageHelper;
import rabbit.sql.page.impl.OraclePageHelper;
import rabbit.sql.page.Pageable;
import rabbit.sql.types.Ignore;
import rabbit.sql.types.Param;
import rabbit.sql.utils.SqlUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Path;
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
    public void xyz() throws Exception{
        String a = "我的";
        String e = new String(Base64.getUrlEncoder().encode(a.getBytes()));
        System.out.println(e);
        String d = new String(Base64.getUrlDecoder().decode(e));
        System.out.println(d);

    }

    @Test
    public void substrTest() throws Exception {
        String s = "aaaaoo";
        System.out.println(s.substring(3, s.length() - 3));
    }

    @Test
    public void nestTest() throws Exception {
        UncheckedCloseable close = null;
        close = UncheckedCloseable.wrap(new FileInputStream("D:\\logs\\debug.log"));
        close = close.nest(new FileInputStream("D:\\logs\\debug-1.log"));
        close = close.nest(new FileInputStream("D:\\logs\\debug-2.log"));
        close = close.nest(new FileInputStream("D:\\logs\\debug-3.log"));

        close.run();
    }

    @Test
    public void numberIn() throws Exception {
        BigDecimal n = new BigDecimal(100);
        System.out.println(Double.parseDouble(n.toString()));
    }

    public static void main(String[] args) throws InterruptedException {
        AtomicInteger x = new AtomicInteger(50);
        List<Runnable> runnables = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
            Runnable runnable = () -> {
                for (int i = 0; i < 10; i++) {
                    x.getAndDecrement();
                }
            };
            runnables.add(runnable);
        }
        runnables.forEach(r -> new Thread(r).start());
        Thread.sleep(1000);
        System.out.println(x.get());
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
        Pair<String, List<String>> pair = SqlUtil.getPreparedSqlAndIndexedArgNames(str);
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

    @Test
    public void paramTest() throws Exception {
        ParamMap map = new ParamMap().putIn("name", "cyx")
                .putIn("age", 21)
                .putIn("address", "昆明市");
        printMap(map);
        System.out.println(ParamMap.empty());
        Map<String, Object> map1 = new HashMap<>();
        map1.put("a", 1);
        map1.put("b", 2);
        map1.put("c", true);

        System.out.println(ParamMap.from(map1));
    }

    public static void printMap(Map<String, Param> map) {
        System.out.println(map);
    }

    @Test
    public void CndTest() throws Exception {
        ICondition condition = Condition.where(Filter.eq("id", 5))
                .and(Filter.gt("age", Wrap.wrapEnd(26, "::text")))
                .or(Filter.endsWith("name", "jack"))
                .and(Filter.gt("id", Wrap.wrapStart("interval", "7 minutes")))
                ;

        Condition xc = Condition.create();

//        Map<String, Param> params = Params.builder()
//                .putIn("name", "cyx")
//                .putIn("age", ValueWrap.wrapEnd("21", "::integer"))
//                .putIn("time", ValueWrap.wrapStart("timestamp", "1993-5-10"))
//                .build();

//        String insert = SqlUtil.generateInsert("test.user", params);
//        String update = SqlUtil.generateUpdate("test.user", params);

        System.out.println(condition.getSql());
        System.out.println(condition.getParams());

        System.out.println(xc.getSql());
        System.out.println(xc.getParams());

//        System.out.println(insert);
//        System.out.println(update);
    }

    @Test
    public void Condition2() throws Exception {
        ICondition condition = Condition.where(Filter.eq("t.id", Wrap.wrapEnd("7", "::integer")))
                .or(Filter.gt("id", Wrap.wrapEnd("100", "::integer")));
        System.out.println(condition.getSql());
        System.out.println(condition.getParams());

        Condition condition1 = Condition.where(Filter.eq("id", Wrap.wrapEnd("7", "::integer")));
        System.out.println(condition1.getSql());
    }

    @Test
    public void generateSql() throws Exception {
        ParamMap paramMap = ParamMap.create().putIn("a", null)
                .putIn("b", "v")
                .putIn("c", "")
                .putIn("d", null)
                .putIn("e", "1");

        System.out.println(SqlUtil.generateInsert("test.user", paramMap, Ignore.BLANK));
    }

    @Test
    public void replaceTest() throws Exception {
        Map<String, Object> args = new HashMap<>();
        args.put("name", Wrap.wrapEnd("cyx", "::text"));
        args.put("age", Wrap.wrapStart("integer", 26));
        args.put("address", "kunming");

        AtomicReference<String> sql = new AtomicReference<>("select * from test.user where id = :id and name = :name or age = :age");

        args.keySet().forEach(k -> {
            if (args.get(k) instanceof Wrap) {
                Wrap v = (Wrap) args.get(k);
                sql.set(sql.get().replace(":" + k, v.getStart() + " :" + k + v.getEnd()));
            }
        });
        System.out.println(sql.get());
    }

    @Test
    public void recordTest() throws Exception {
        Record record = new Record();
        record.put("name", "cyx");
        System.out.println(record.getString("age"));
    }

    @Test
    public void testFile() throws IOException, URISyntaxException {
        Path p = ResourceUtil.getClassPathResources("pgsql/data.sql", ".sql")
                .findFirst().get();
        System.out.println(p);
    }

    @Test
    public void javaType() throws Exception {
        byte[] bytes = new byte[1];
        System.out.println(Date.class.getName());

        String[] names = new String[]{"name", "age", "boy"};
        Object[] values = new Object[]{"chengyuxing", 26, true};
        DataRow row = DataRow.of(names, values);
        System.out.println(row);
        System.out.println(SqlUtil.generateUpdate("test.user", row.toMap(Param::IN)) + Condition.where(Filter.eq("id", 2)).getSql());
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
    public void pageTest() throws Exception {
        PageHelper page = OraclePageHelper.of(12, 10);
        page.init(100);
        Pageable<Integer> pageable = Pageable.of(page, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        System.out.println(pageable);
        System.out.println(Json.toJson(pageable));
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writeValueAsString(pageable));
    }
}
