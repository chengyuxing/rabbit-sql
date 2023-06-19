package tests;

import com.github.chengyuxing.sql.utils.SqlTranslator;
import org.junit.Test;

import java.util.Collections;

public class SimpleTests {
    @Test
    public void test1() throws Exception {
        System.out.println("ababab".replace("ba", "$"));
        System.out.println(new SqlTranslator(':').generateSql("id = id and name = name", Collections.emptyMap(), true));
    }
}
