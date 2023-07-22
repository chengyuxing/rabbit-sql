package sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.Keywords;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.Arrays;

public class TestStrTemplate {
    static final String sql = "select ${ fields } from test.user where ${  cnd} ;";
    static final Args<Object> args = Args.<Object>of("ids", Arrays.asList("I'm Ok!", "b", "c"))
            .add("fields", "id, name, age")
            .add("id", Math.random())
            .add("date", "2020-12-23 ${:time}")
            .add("time", "11:23:44")
            .add("cnd", "id in (${ :ids.0 },${fields}) and id = :id or ${date} '${mn}' and ${");

    static SqlGenerator sqlGenerator = new SqlGenerator(':');

    @Test
    public void testOld() throws Exception {
        System.out.println(SqlUtil.formatSql(sql, args));
        System.out.println("---");
    }

    @Test
    public void testO() throws Exception {
        boolean res = Object[].class.isAssignableFrom(Integer[].class);
    }

    @Test
    public void test42() throws Exception {
        System.out.println(StringUtil.equalsAnyIgnoreCase("DESC", Keywords.STANDARD));
        System.out.println(StringUtil.equalsAnyIgnoreCase("abc", "1"));
    }

    @Test
    public void test43() throws ClassNotFoundException {
        FileResource.getClassLoader().loadClass("com.github.chengyuxing.sql.XQLFileManager");
    }
}
