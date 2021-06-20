package tests;

import com.github.chengyuxing.sql.Args;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SingleTests {
    private static List<Map<String, Object>> list;

    @BeforeClass
    public static void init() {
        list = new ArrayList<>();
        for (int i = 0; i < 16000; i++) {
            Map<String, Object> map = Args.<Object>of("id", i)
                    .add("name", "chengyuxing")
                    .add("address", "kunming")
                    .add("no", i);
            list.add(map);
        }
    }

    @Test
    public void speedRemove() throws Exception {
        Iterator<Map<String, Object>> iterator = list.iterator();
        while (iterator.hasNext()) {
            Map<String, Object> next = iterator.next();
            iterator.remove();
        }
        System.out.println(list.size());
    }
}
