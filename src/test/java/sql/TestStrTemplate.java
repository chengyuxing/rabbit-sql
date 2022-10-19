package sql;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import org.junit.Test;

import java.util.Arrays;

public class TestStrTemplate {
    static final String sql = "select ${ fields } from test.user where ${  CND}";
    static final Args<Object> args = Args.<Object>of("ids", Arrays.asList("I'm Ok!", "b", "c"))
            .add("fields", "id, name, age")
            .add("id", Math.random())
            .add("ids", Arrays.asList("a", "b", "c"))
            .add("date", "2020-12-23 ${:time}")
            .add("time", "11:23:44")
            .add("cnd", "id in (${:ids},${fields}) and id = :id or ${date} '${mn}' and ${");

    static SqlTranslator sqlTranslator = new SqlTranslator(':');

    @Test
    public void testOld() throws Exception {
        System.out.println(sqlTranslator.resolveSqlStrTemplate(sql, args));
    }
}
