package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.*;
import com.github.chengyuxing.sql.types.OutParamType;
import com.github.chengyuxing.sql.transaction.Tx;
import com.github.chengyuxing.sql.types.StandardOutParamType;
import com.github.chengyuxing.sql.types.Param;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;


public class MyTest {

    private static BakiDao baki;
    private static HikariDataSource dataSource;

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");

        XQLFileManager manager = new XQLFileManager();
        manager.add("data", "dynamic-sql-example/choose.xql");

        BakiDao bakiDao = new BakiDao(dataSource);
        bakiDao.setXqlFileManager(manager);
        baki = bakiDao;
    }

    @Test
    public void toEntity() throws Exception {
        baki.query("select * from test.tb")
                .findFirst()
                .ifPresent(row -> {
                    Tb tb = (Tb) row.toEntity(Tb.class);
                    System.out.println(tb);
//                    System.out.println(tb.getStrs().get(0));
                    System.out.println(tb.getDt());
                });
    }

    @Test
    public void testFieldCase() throws Exception {
        baki.query("select 1 A, 2 \"B\", current_date DT, now() \"NoW\"")
                .stream()
                .forEach(System.out::println);
    }


    @Test
    public void defaultPager() throws Exception {
        PagedResource<DataRow> pagedResource = baki.query("select * from test.region")
                .pageable(1, 5)
                .count(10)
                .collect(d -> d);
        System.out.println(pagedResource);
    }

    @Test
    public void query() throws Exception {
        try (Stream<DataRow> s = baki.query("select ${fields} from test.region where id = :id")
                .args(DataRow.of("fields", Arrays.asList("name", "pid"), "id", 11))
                .stream()) {
            s.forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void pagerTest() throws Exception {
        PagedResource<DataRow> res = baki.query("&data.custom_paged")
                .pageable(1, 7)
                .args(DataRow.of("id", 8))
                .disableDefaultPageSql("select count(*) from test.region where id > :id")
//                .pageHelper(new MysqlPageHelper(){
//                    @Override
//                    public String pagedSql(String sql) {
//                        return super.pagedSql(sql);
//                    }
//                })
                .collect(d -> d);
        res.getData().forEach(System.out::println);
    }

    @Test
    public void line() throws Exception {
        DataRow row = baki.execute("do\n" +
                "$$\n" +
                "    declare\n" +
                "        x    integer[];\n" +
                "        nums integer[] := array [[1,2,3],[4,5,6],[7,8,9]];\n" +
                "    begin\n" +
                "        foreach x slice 1 in array nums\n" +
                "            loop\n" +
                "                raise warning 'num:%',x;\n" +
                "            end loop;\n" +
                "    end;\n" +
                "\n" +
                "$$;", Args.of());
        System.out.println(row);
    }

    @Test
    public void array() throws Exception {
        baki.query("select array [12,13,11,4,5,6.7]:: varchar[], '{\"a\":\"chengyuxing\"}'::jsonb")
                .findFirst()
                .ifPresent(r -> {
                    System.out.println(r);
                    System.out.println(r.getFirst());

//                        Array arr = r.get(0);
//                        Integer[] ints = (Integer[]) arr.getArray();
//                        System.out.println(Arrays.toString(ints));
                });
    }

    @Test
    public void loadData() throws Exception {
        baki.execute("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','", Args.of());
//        baki.execute("copy test.fruit from '/Users/chengyuxing/test/fruit2.txt' with delimiter ','");
    }

    @Test
    public void testX() throws Exception {
//        Tx.begin();
        try (Stream<DataRow> s = baki.query("select now()").stream().distinct()) {
            s.forEach(System.out::println);
        }
        while (true) {

        }
    }

    @Test
    public void queryInTransaction() throws Exception {
        Tx.using(() -> {
            baki.query("select current_timestamp, version()")
                    .findFirst()
                    .ifPresent(System.out::println);
//            baki.insert("test.history").save(DataRow.of("userid", UUID.randomUUID(), "words", "transactional"));
        });
    }

    @Test
    public void using() throws Exception {
        try {
            baki.using(connection -> {
                try {
                    PreparedStatement statement = connection.prepareStatement("select current_timestamp, version()");
                    ResultSet resultSet = statement.executeQuery();
                    while (resultSet.next()) {
                        System.out.println(resultSet.getObject(1));
                    }
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
                return 1;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void multi_res_function() throws Exception {
        Tx.using(() -> {
            Map<String, Param> paramMap = new HashMap<>();
            paramMap.put("success", Param.OUT(StandardOutParamType.BOOLEAN));
            paramMap.put("res", Param.OUT(StandardOutParamType.REF_CURSOR));
            paramMap.put("msg", Param.OUT(StandardOutParamType.VARCHAR));
            DataRow row = baki.call("{call test.multi_res(12, :success, :res, :msg)}", paramMap);
            System.out.println(row);
        });
    }

    @Test
    public void callTest() throws Exception {
        class TIME implements OutParamType {

            @Override
            public int typeNumber() {
                return Types.TIME;
            }

            public String getName() {
                return "time";
            }
        }
        Map<String, Param> params = new HashMap<>();
        params.put("dt", Param.OUT(StandardOutParamType.TIMESTAMP));
        params.put("a", Param.IN(11));
        params.put("b", Param.IN(22));
        params.put("sum", Param.OUT(StandardOutParamType.INTEGER));
        params.put("tm", Param.OUT(new TIME()));
        DataRow row = baki.call("{call test.fun_now(:a, :b, :sum, :dt, :tm)}", params);
        Timestamp dt = row.getAs("dt");
        System.out.println(dt.toLocalDateTime());
        System.out.println(row);
        System.out.println((int) row.get("sum"));
        System.out.println((Time) row.get("tm"));
        PGobject obj = new PGobject();
    }
}
