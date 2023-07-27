package strtests;

import com.github.chengyuxing.common.WatchDog;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class StrTests {

    public static void main(String[] args) {
        WatchDog watchDog = new WatchDog();
        watchDog.addListener("tick", () -> {
            System.out.println(LocalDateTime.now());
        }, 1, TimeUnit.SECONDS);
    }

    public static String sql = "/*我的注释*/\n" +
            "select count(*)\n" +
            "from test.student t\n" +
            "WHERE\n" +
            "--#if :age !=null\n" +
            "    t.age > 21\n" +
            "--#fi\n" +
            "--#if :name != null\n" +
            "  and t.name ~ :name\n" +
            "--#fi\n" +
            "--#if :age <> blank && :age < 90\n" +
            "and age < 90\n" +
            "--#fi\n" +
            ";";

    @Test
    public void sqlTest() throws Exception {
//        System.out.println(removeAnnotationBlock(sql));
//        System.out.println("2021-05-24 WEEK 17:32:00");
//        System.out.println(LocalDateTime.now().getDayOfWeek());
        System.out.println(LocalDateTime.now().getDayOfYear());
        System.out.println(Clock.systemDefaultZone());
    }

    @Test
    public void testLineAnno() throws Exception {
        String lineSql1 = "'hello' --world be 'happy' 'annotation'";
        String lineSql2 = "--hello world be 'happy' 'annotation'";
        String lineSql3 = "hello --world be 'happy' --'annotation'";
        Args<Object> args = Args.<Object>of("a", 1)
                .add("name", null)
                .add("b", "2")
                .add("c", null);
        args.removeIfAbsentExclude("name");
        System.out.println(args);
    }

    @Test
    public void dt() throws Exception {
        WatchDog watchDog = new WatchDog();
        watchDog.addListener("tick", () -> {
            System.out.println(LocalDateTime.now());
        }, 1, TimeUnit.SECONDS);
    }

    @Test
    public void ExcludePlaceholder() throws Exception {
        String sql = "select '${tableName}',${age} age from test.${tableName} where id = 10";
        Args<Object> args = Args.<Object>of("${tableName}", "user").add("${age}", 28);
//        System.out.println(SqlUtil.resolveSqlPart(sql, args));
    }

    @Test
    public void testSqlFix() {
        System.out.println(SqlUtil.repairSyntaxError("update test.user set id = :id,\nname=:name,\nwhere order by id desc"));
    }
}
