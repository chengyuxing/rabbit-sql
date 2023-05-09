package tests;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.PageHelperProvider;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.util.Collections;
import java.util.Map;

public class SqliteTests {

    private static HikariDataSource dataSource;
    private static Baki baki;

    @BeforeClass
    public static void init() {
        dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setJdbcUrl("jdbc:sqlite:/Users/chengyuxing/Downloads/sqlite_data");
        BakiDao bakiDao = BakiDao.of(dataSource);
        bakiDao.setPageHelperProvider(new PageHelperProvider() {
            @Override
            public PageHelper customPageHelper(DatabaseMetaData databaseMetaData, String dbName, char namedParamPrefix) {
                if (dbName.equals("sqlite")) {
                    return new PageHelper() {
                        @Override
                        public String pagedSql(String sql) {
                            return sql + " limit 10 offset 0";
                        }

                        @Override
                        public Map<String, Integer> pagedArgs() {
                            return null;
                        }
                    };
                }
                return null;
            }
        });
        baki = bakiDao;
    }

    @Test
    public void test1() {
        PagedResource<DataRow> resource = baki.query("select 1 union select 2")
                .pageable(1, 10)
                .collect();

        System.out.println(resource);
    }
}
