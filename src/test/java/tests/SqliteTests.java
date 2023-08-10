package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.DateTimes;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqliteTests {

    private static HikariDataSource dataSource;
    private static Baki baki;

    @BeforeClass
    public static void init() {
        dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setJdbcUrl("jdbc:sqlite:/Users/chengyuxing/Downloads/sqlite_data");
        BakiDao bakiDao = new BakiDao(dataSource);
//        bakiDao.setGlobalPageHelperProvider(new PageHelperProvider() {
//            @Override
//            public PageHelper customPageHelper(DatabaseMetaData databaseMetaData, String dbName, char namedParamPrefix) {
//                if (dbName.equals("sqlite")) {
//                    return new PageHelper() {
//                        @Override
//                        public String pagedSql(String sql) {
//                            return sql + " limit 10 offset 0";
//                        }
//
//                        @Override
//                        public Map<String, Integer> pagedArgs() {
//                            return null;
//                        }
//                    };
//                }
//                return null;
//            }
//        });
        baki = bakiDao;
    }

    @Test
    public void test1() {
        PagedResource<DataRow> resource = baki.query("select 1 union select 2")
                .pageable(1, 10)
                .rewriteDefaultPageArgs(a -> {
                    a.updateKey("limit", "l");
                    a.updateKey("offset", "o");
                    return a;
                })
                .pageHelper((databaseMetaData, dbName, namedParamPrefix) -> new PGPageHelper())
                .collect();

        System.out.println(resource);
    }

    @Test
    public void test2() {
        baki.query("select 1 union select 2 where id = :id")
                .arg("id", 10)
                .findFirst()
                .ifPresent(System.out::println);
    }

    @Test
    public void test34() {
        baki.of("select 1 union select 2 where id = :id");
    }

    @Test
    public void test35() {
        baki.update("test.user", "id = :id")
                .save(DataRow.of("id", 10, "name", "cyx"));

        Args<Object> args = Args.of("dt", "2022-1-2");
        args.updateValue("dt", v -> DateTimes.toLocalDateTime(v.toString()));
        args.updateKey("dt", "date");
    }

    @Test
    public void test11() {
        try (Stream<DataRow> s = baki.query("&users.top100").stream()) {
            List<User> users = s.map(d -> d.toEntity(User.class))
                    .peek(System.out::println)
                    .filter(u -> u.getAge() > 18)
                    .filter(u -> u.getName().equals("admin"))
                    .collect(Collectors.toList());
        }
    }
}
