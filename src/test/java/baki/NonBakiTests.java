package baki;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.expression.Patterns;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.XQLFileManagerConfig;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.types.Variable;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlHighlighter;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NonBakiTests {
    @Test
    public void test1() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.setDatabaseId("oracle");
        xqlFileManager.add("pgsql/other.sql");
        xqlFileManager.add("http://localhost:8080/share/cyx.xql");
        xqlFileManager.init();
        System.out.println("---");
        System.out.println(xqlFileManager.get("other.ooooooooo", Args.of()));
        System.out.println("---");
        System.out.println(xqlFileManager.get("other.other", Args.of("id", 1, "name", "null")));
    }

    @Test
    public void test2() {
        PageHelper pageHelper = new PGPageHelper();
        pageHelper.init(1, 15, 45);
        PagedResource<DataRow> pagedResource = PagedResource.of(pageHelper, Collections.singletonList(DataRow.of("a", 1, "b", 2)));
//        String j = Jackson.toJson(pagedResource);
//        System.out.println(j);
    }

    @Test
    public void testArgs() {
        Args<Object> args = Args.of("name", "cyx", "age", 30, "date", "2023-8-4 22:45", "info", Args.of("address", "kunming"));
        args.updateKey("name", "NAME");
        args.updateKeys(String::toUpperCase);
        System.out.println(args);
        System.out.println(DataRow.of());
    }

    @Test
    public void highlightSqlTest() {
        // language=SQL
        String sql = "/*[ooooooooo]*/\n" +
                "select count(*),\n" +
                "       count(*) filter ( where grade > :g1 )               greate,\n" +
                "       -- #if :databaseId != blank\n" +
                "        test.string_agg (id, ', ') /*filter ( where grade < :g2 and grade > :g3)*/ good,\n" +
                "       -- #fi\n" +
                "       count(*) filter /*( where grade < 60 )               bad\n" +
                "from test.score where id::number = :id*/;";

        SqlGenerator sqlGenerator = new SqlGenerator(':');
        Pair<String, Map<String, List<Integer>>> pair = sqlGenerator.generatePreparedSql(sql, Collections.emptyMap());
        System.out.println(pair.getItem1());
        System.out.println(pair.getItem2());

        Matcher m = sqlGenerator.getNamedParamPattern().matcher(sql);
        while (m.find()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                System.out.println("Group " + i + ": " + m.group(i));
            }
        }

        String proc = "{call test.func1(:id)}";

        String b = SqlHighlighter.ansi(sql);
        System.out.println(System.console());
        System.out.println(b);

        String c = SqlHighlighter.ansi(proc);
        System.out.println(c);

        System.out.println(System.getenv("TERM"));
    }

    static final String query = "select t.id || 'number' || 'age:age,name:cyx', '{\"name\":\"user\"}'::jsonb from test.user where id =:integer::integer and id >:idc or id < :idc and name=text :username";
    static final String insert = "insert into test.user(idd,name,id,age,address) values (*id,*name::integer,*idd::float,integer *age,date *address)";

    @Test
    public void testPs3() {
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        Pair<String, Map<String, List<Integer>>> pair1 = sqlGenerator.generatePreparedSql(query, Args.of(
                "id", 25,
                "idc", 15,
                "username", "cyx"
        ));
        System.out.println(pair1.getItem1());
        System.out.println(pair1.getItem2());
    }

    @Test
    public void test23() {
        SqlGenerator sqlGenerator = new SqlGenerator('*');
        System.out.println(sqlGenerator.getNamedParamPattern());
        Pair<String, Map<String, List<Integer>>> sqla = sqlGenerator.generatePreparedSql(insert, Args.of("id", 12,
                "name", "chengyuxing",
                "idd", 16,
                "age", 30,
                "address", LocalDateTime.now()));
        System.out.println(sqla.getItem1());
        System.out.println(sqla.getItem2());
    }

    @Test
    public void testSqlFormat() {
        System.out.println(SqlUtil.formatSql("select *, '${now}' as now from test.user where dt < ${!current}",
                Args.of("now", LocalDateTime.now(),
                        "current", LocalDateTime.now())));
    }

    @Test
    public void testSqlErrorFix() {
        String sql = "select * from user\nwhere and(id = :id)";
        String sql4 = "select * from user\nwhere \n\t\nand id = :id";
        String sql2 = "select * from user where order by id desc";
        String sql3 = "update test.user set name = :name ,  where id = 1";
        System.out.println(SqlUtil.repairSyntaxError(sql));
        System.out.println(SqlUtil.repairSyntaxError(sql4));
        System.out.println(SqlUtil.repairSyntaxError(sql2));
        System.out.println(SqlUtil.repairSyntaxError(sql3));
    }

    @Test
    public void testJsonDate() throws JsonProcessingException {
        JavaTimeModule module = new JavaTimeModule();
        LocalDateTimeDeserializer dateTimeDeserializer = new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTimeSerializer dateTimeSerializer = new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        module.addDeserializer(LocalDateTime.class, dateTimeDeserializer);
        module.addSerializer(LocalDateTime.class, dateTimeSerializer);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModules(module);
        System.out.println(mapper.writeValueAsString(Args.of("now", LocalDateTime.now())));
        System.out.println(Module[].class.getName());
    }

    @Test
    public void testSqlA() {
        String sql = "select t.id || 'number' || 'name:cyx','{\"name\": \"user\"}'::jsonb\n" +
                "from test.user t\n" +
                "where id = :id::integer --suffix type convert\n" +
                "and id {@code >} :idc\n" +
                "and name = text :username --prefix type convert\n" +
                "and '[\"a\",\"b\",\"c\"]'::jsonb{@code ??&} array ['a', 'b'] ${cnd};";
        System.out.println(sql);
        System.out.println("----");
        boolean singleSubstring = false;
        boolean named = false;
        String sb = "";
        char[] chars = sql.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (singleSubstring) {
                if (c == '\'') {
                    singleSubstring = false;
                }
                continue;
            }

            if (c == '\'') {
                singleSubstring = true;
                continue;
            }

            if (c == ':') {
                if (named) {
                    named = false;
                } else
                    named = true;
                continue;
            }
            if (c < 'a' || c > 'z') {
                if (named) {
                    System.out.println(sb);
                    sb = "";
                    named = false;
                }
                continue;
            }
            if (named) {
                sb += c;
            }
        }

        System.out.println("----");
    }

    @Test
    public void testPath() {
        Path p = Paths.get("/Users/chengyuxing/Downloads/jdk19.excel.demo.xlsx");
        Path p2 = Paths.get("/Users/chengyuxing/Downloads/jdk19.excel.demo.xlsx");

        System.out.println(p.hashCode());
        System.out.println(p2.hashCode());

        System.out.println(p.getFileName());
        System.out.println(p.endsWith(Paths.get("Downloads", "jdk19.excel.demo.xlsx")));
        System.out.println(p.endsWith("jdk19.excel.demo.xlsx"));
        System.out.println(Paths.get("/Users/chengyuxing/Downloads").getFileName());
    }

    @Test
    public void test44() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("pgsql/b.a.xql");
        xqlFileManager.init();
        String sql = xqlFileManager.get("b.a.queryUsers");
        System.out.println(sql);
    }

    @Test
    public void test45() {
        String sql = "select * from test.user where id = :id and dt = ${!now} and o = ${!now}";
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        sqlGenerator.setTemplateFormatter((v, b) -> {
            if (b) {
                return SqlUtil.safeQuote(v.toString());
            }
            return v.toString();
        });
        System.out.println(sqlGenerator.generateSql(sql, Args.of("id", null, "now", LocalDateTime.now())));
    }

    @Test
    public void test46() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.add("sys", "pgsql/system.xql");
        xqlFileManager.add("xstj", "pgsql/xstjfx.xql");
        xqlFileManager.init();

        Map<String, XQLFileManager.Resource> resourceMap = xqlFileManager.getResources();
        System.out.println(resourceMap);
//        Pair<String, Map<String, Object>> pair = xqlFileManager.get("sys.queryUserByPassword", Args.of("username", "abc"));
//        System.out.println(SqlUtil.repairSyntaxError(pair.getItem1()));
    }

    @Test
    public void testYml() {
        XQLFileManagerConfig config = new XQLFileManagerConfig();
        config.loadYaml(new FileResource("xql-file-manager.old.yml"));
        System.out.println(config);
    }

    @Test
    public void testR() {
        String a = "id = :id.item and id <> :id3uij and id > :id::id";
        Pattern p = new SqlGenerator(':').getNamedParamPattern();
        System.out.println(p.pattern());
        StringBuilder sb = new StringBuilder();
        Matcher m = p.matcher(a);
        int lastMatchEnd = 0;
        while (m.find()) {
            String name = m.group(1);
            sb.append(a, lastMatchEnd, m.start());
            if (name != null && name.equals("id")) {
                sb.append("_for.").append(name).append("_0_1");
            } else {
                sb.append(m.group());
            }
            lastMatchEnd = m.end();
        }
        sb.append(a.substring(lastMatchEnd));
        System.out.println(sb);
    }

    @Test
    public void testX() {
        XQLFileManager xqlFileManager=new XQLFileManager();
        String sql = "--#for id of :list delimiter ' or '\n"+
                "t.id like '%' || :iad || '%'\n"+
                "--#done";
        System.out.println(xqlFileManager.parseDynamicSql(sql, Args.of("list", Arrays.asList("a", "b", "c"))));
    }
}
