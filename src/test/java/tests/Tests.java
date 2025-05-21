package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.MostDateTime;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.page.impl.OraclePageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.util.*;

public class Tests {

    @Test
    public void testHash() {
        System.out.println(StringUtil.hash("", "MD5"));
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
    public void dtTest2() throws Exception {
        String ts = "12月11 23:12:55";
        String dt = "2020-12-11";
        String tm = "23时12分55秒";

        System.out.println(MostDateTime.toDate(ts));
        System.out.println(MostDateTime.now().toString("yyyy-MM-dd"));
    }

    @Test
    public void sqlReplace() throws Exception {
        String str = "select t.id || 'number' || 'age:age,name:cyx', '{\"name\":\"user\"}'::jsonb from test.user where id =:id::integer and id >:idc and name=text :username";
        String sql = "insert into test.user(idd,name,id,age,address) values (:id,:name::integer,:idd::float,integer :age,date :address)";
        SqlGenerator.GeneratedSqlMetaData pair = new SqlGenerator(':').generatePreparedSql(str, Collections.emptyMap());
        System.out.println(pair.getPrepareSql());
        System.out.println(pair.getArgs());
    }

    @Test
    public void sqlPlaceHolder() throws Exception {
        String query = "select * from test where id = ?_i.d and id = ?id and idCard = '5301111' or name = ?na_me ${cnd}";
        SqlGenerator.GeneratedSqlMetaData sql = new SqlGenerator('?').generatePreparedSql(query, DataRow.of("cnd", "and date <= '${date}'")
                .add("date", "2020-12-23 ${time}")
                .add("time", "11:23:44"));
        System.out.println(sql.getPrepareSql());
        System.out.println(sql.getArgs());
    }

    @Test
    public void trimEnds() throws Exception {
        System.out.println(SqlUtil.trimEnd("where id = 10\r\n;  ;;;\t\r\n"));
    }

    @Test
    public void pageTest() throws Exception {
        OraclePageHelper page = new OraclePageHelper();
        page.init(1, 10, 100);
        System.out.println(page.start());
        System.out.println(page.end());
        System.out.println(page.pagedSql(':', "select * from test.user"));
    }
}
