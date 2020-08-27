package tests;

import org.junit.Test;
import rabbit.sql.dao.SQLFileManager;
import rabbit.sql.utils.SqlUtil;

public class SqlFileTest {

    @Test
    public void andConcat() throws Exception {
        String sql = "...from user where '1' = '1' and\n ${cnd}";
        String cnd = "and t.id = 12";

        boolean exists = prevKeywordContains(sql, "${cnd}", "and");
        System.out.println(exists);

    }

    /**
     * 从指定开始字符串开始向前查找
     * 从第一个非(空格\n\t)字符开始往前找第一个指定关键字，是否包含指定的关键字
     *
     * @param str     字符串
     * @param from    开始字符串
     * @param keyword 要查找的关键字
     * @return 是否包含
     */
    public static boolean prevKeywordContains(String str, String from, String keyword) {
        int idx = str.indexOf(from) - 1;
        int len = keyword.length();
        StringBuilder sb = new StringBuilder();
        int x = 0;
        for (int i = idx; i > 0; i--) {
            char c = str.charAt(i);
            if (x > 0) {
                sb.insert(0, c);
                x++;
                if (x == len) {
                    break;
                }
                continue;
            }
            if (c != ' ' && c != '\n' && c != '\t') {
                sb.insert(0, c);
                x++;
            }
        }
        return sb.toString().equals(keyword);
    }

    @Test
    public void sqlTest() throws Exception {
        SQLFileManager sqlFileManager = new SQLFileManager("pgsql/data.sql", "pgsql/other.sql");
        sqlFileManager.init();
        sqlFileManager.look();
        System.out.println("------------");
        System.out.println(sqlFileManager.get("pgsql.data.logical"));

    }

    @Test
    public void strTest() throws Exception {
        String sql = "select * from user where 1=1 and id = 2)}];\n;; \t\n \r\n;; \n\n\t\r";
//        System.out.println(SqlUtil.trimEnd(sql));
        System.out.println(sql.trim());
    }
}
