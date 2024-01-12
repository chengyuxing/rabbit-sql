package baki;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.Jackson;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public class NonBakiTests {
    @Test
    public void test1() {
        XQLFileManager xqlFileManager = new XQLFileManager();
        xqlFileManager.setDatabaseId("oracle");
        xqlFileManager.add("pgsql/other.sql");
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
        String j = Jackson.toJson(pagedResource);
        System.out.println(j);
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
        String sql = "select count(*),\n" +
                "       count(*) filter ( where grade > 90 )               greate,\n" +
                "       -- #if :_databaseId != blank\n" +
                "        count(*) filter ( where grade < 90 and grade > 60) good,\n" +
                "       -- #fi\n" +
                "       count(*) filter ( where grade < 60 )               bad\n" +
                "from test.score;";

        System.out.println(SqlUtil.highlightSql(sql));
    }

    static final String query = "select t.id || 'number' || 'age:age,name:cyx', '{\"name\":\"user\"}'::jsonb from test.user where id =:id::integer and id >:idc and name=text :username";
    static final String insert = "insert into test.user(idd,name,id,age,address) values (:id,:name::integer,:idd::float,integer :age,date :address)";

    @Test
    public void testSqlParamResolve2() {
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        System.out.println("--old---");
        Pair<String, List<String>> pairQ = sqlGenerator.generatePreparedSql(query, Collections.emptyMap());
        System.out.println(pairQ.getItem1());
        System.out.println(pairQ.getItem2());

        System.out.println("--old---");
        Pair<String, List<String>> pairI = sqlGenerator.generatePreparedSql(insert, Collections.emptyMap());
        System.out.println(pairI.getItem1());
        System.out.println(pairI.getItem2());

    }

    @Test
    public void test23() {
        SqlGenerator sqlGenerator = new SqlGenerator(':');
        Pair<String, List<String>> sqla = sqlGenerator.generatePreparedSql(insert, Args.of("id", 12,
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
        String sql4 = "select * from user\nwhere and id = :id";
        String sql2 = "select * from user where order by id desc";
        String sql3 = "update test.user set name = :name ,  where id = 1";
        System.out.println(SqlUtil.repairSyntaxError(sql));
        System.out.println(SqlUtil.repairSyntaxError(sql4));
        System.out.println(SqlUtil.repairSyntaxError(sql2));
        System.out.println(SqlUtil.repairSyntaxError(sql3));
    }
}
