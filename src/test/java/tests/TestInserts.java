package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.common.types.DataRow;
import rabbit.sql.Baki;
import rabbit.sql.dao.BakiDao;
import rabbit.sql.types.DataFrame;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class TestInserts {
    static Baki baki;
    static HikariDataSource dataSource;

    static List<DataRow> rows = new ArrayList<>();

    @BeforeClass
    public static void init() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        dataSource.setUsername("chengyuxing");
        dataSource.setDriverClassName("org.postgresql.Driver");
        baki = BakiDao.of(dataSource);

        for (int i = 0; i < 10000; i++) {
            DataRow row = DataRow.fromPair("id", i,
                    "name", "chengyuxing",
                    "words", "it's my time!",
                    "dts", LocalDateTime.now());
            rows.add(row);
        }
    }

    @Test
    public void normal() throws Exception {
        baki.insert(DataFrame.ofRows("test.message", rows));
    }

    @Test
    public void batch() throws Exception {
        baki.fastInsert(DataFrame.ofRows("test.message", rows));
    }
}
