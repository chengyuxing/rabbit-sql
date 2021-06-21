package tests;

import EntityTests.RegionTree;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.MenuTree;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.BakiDao;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TreezTests {
    private static BakiDao baki;
    private static List<Map<String, Object>> treeData1 = new ArrayList<>();
    private static List<Map<String, Object>> treeData2 = new ArrayList<>();
    private static List<RegionTree> treezData3 = new ArrayList<>();

    private static final AtomicInteger tree1Count = new AtomicInteger();
    private static final AtomicInteger tree2Count = new AtomicInteger();

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");
        baki = BakiDao.of(dataSource);
        treeData1 = baki.queryMaps("select id,name,pid,'bool' as status from test.region");
        treeData2 = baki.queryMaps("select id,name,pid from test.region");
        try (Stream<DataRow> s = baki.query("select id,name,pid from test.region order by id;")) {
            treezData3 = s.map(r -> new RegionTree(r.toMap())).collect(Collectors.toList());
        }
    }

    @Test
    public void x2e() throws Exception {
        treeData1.forEach(m -> System.out.println(new RegionTree(m)));
    }

    @Test
    public void mergeTree1() throws Exception {
        Map<String, Object> tree = new HashMap<>();
        tree.put("id", 0);
        tree.put("pid", -1);
        tree.put("name", "地球");
        tree.put("children", new ArrayList<>());
        mergeTree1(treeData1, tree);
        System.out.println(ReflectUtil.obj2Json(tree));
        System.out.println(treeData1.size());
        System.out.println("tree1执行递归次数：" + tree1Count.get());
    }

    @Test
    public void mergeTree2() throws Exception {
        Map<String, Object> tree = new HashMap<>();
        tree.put("id", 0);
        tree.put("pid", -1);
        tree.put("name", "地球");
        tree.put("children", new ArrayList<>());
        List<Map<String, Object>> trees = new ArrayList<>();
        trees.add(tree);
        mergeTree2(treeData2, trees);
        System.out.println(ReflectUtil.obj2Json(trees));
        System.out.println(treeData2.size());
        System.out.println("tree2执行递归次数：" + tree2Count.get());
    }

    @Test
    public void mergeTree3() throws Exception {
        RegionTree root = new RegionTree(Args.of("id", 0).add("pid", -999));
        MenuTree.Tree trees = new MenuTree(treezData3).create(root);
        System.out.println(ReflectUtil.obj2Json(trees));
        System.out.println(treezData3.size());
        System.out.println(root);
    }

    @SuppressWarnings("unchecked")
    public static void mergeTree1(List<Map<String, Object>> list, Map<String, Object> tree) {
        tree1Count.incrementAndGet();
        Iterator<Map<String, Object>> iterator = list.iterator();
        List<Map<String, Object>> children = (List<Map<String, Object>>) tree.get("children");
        if (children == null) {
            children = new ArrayList<>();
            tree.put("children", children);
        }
        while (iterator.hasNext()) {
            Map<String, Object> next = iterator.next();
            if (tree.get("id").equals(next.get("pid"))) {
                next.put("children", new ArrayList<>());
                children.add(next);
                iterator.remove();
            }
        }
        for (Map<String, Object> child : children) {
            mergeTree1(list, child);
        }
    }

    @SuppressWarnings("unchecked")
    public static void mergeTree2(List<Map<String, Object>> list, List<Map<String, Object>> tree) {
        tree2Count.incrementAndGet();
        if (list.isEmpty()) {
            return;
        }
        for (int i = tree.size() - 1, j = 0; i >= j; i--, j++) {
            Iterator<Map<String, Object>> iterator = list.iterator();
            while (iterator.hasNext()) {
                Map<String, Object> next = iterator.next();
                Map<String, Object> backward = tree.get(i);
                Map<String, Object> forward = tree.get(j);
                if (forward.get("id").equals(next.get("pid"))) {
                    next.put("children", new ArrayList<>());
                    ((List<Map<String, Object>>) forward.get("children")).add(next);
                    iterator.remove();
                } else if (backward.get("id").equals(next.get("pid"))) {
                    next.put("children", new ArrayList<>());
                    ((List<Map<String, Object>>) backward.get("children")).add(next);
                    iterator.remove();
                }
            }
            mergeTree2(list, (List<Map<String, Object>>) tree.get(j).get("children"));
        }
    }
}
