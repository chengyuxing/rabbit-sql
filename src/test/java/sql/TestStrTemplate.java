package sql;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.Arrays;

public class TestStrTemplate {
    static final String sql = "select ${ fields } from test.user where ${  cnd}";
    static final Args<Object> args = Args.<Object>of("ids", Arrays.asList("I'm Ok!", "b", "c"))
            .add("fields", "id, name, age")
            .add("id", Math.random())
            .add("date", "2020-12-23 ${:time}")
            .add("time", "11:23:44")
            .add("cnd", "id in (${ :ids.0 },${fields}) and id = :id or ${date} '${mn}' and ${");

    static SqlTranslator sqlTranslator = new SqlTranslator(':');

    @Test
    public void testOld() throws Exception {
        System.out.println(sqlTranslator.formatSql(sql, args));
        System.out.println("---");
    }

    @Test
    public void testO() throws Exception {
        System.out.println(sqlTranslator.formatSql("${a.b}",Args.create("a","b")));
    }

    @Test
    public void test42() throws Exception{
        System.out.println(SqlUtil.deconstructArrayIfNecessary(null,false));
    }
}
