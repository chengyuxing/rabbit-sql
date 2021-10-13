package tests;

import entity.PMode;
import entity.PValue;
import org.junit.Test;

public class StoreArgTests {
    @Test
    public void test1() throws Exception {
        PValue value = PMode.IN.put("abc");
        System.out.println(value);
    }
}
