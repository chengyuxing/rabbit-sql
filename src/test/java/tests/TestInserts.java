package tests;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import rabbit.common.types.DataRow;
import rabbit.sql.Baki;
import rabbit.sql.dao.Args;
import rabbit.sql.dao.BakiDao;
import rabbit.sql.types.DataFrame;
import rabbit.sql.utils.SqlUtil;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

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

//        for (int i = 0; i < 10000; i++) {
//            DataRow row = DataRow.fromPair("id", i,
//                    "name", "chengyuxing",
//                    "words", "it's my time!",
//                    "dts", LocalDateTime.now());
//            rows.add(row);
//        }
    }

    @Test
    public void normal() throws Exception {
        baki.insert(DataFrame.ofRows("test.message", rows));
    }

    @Test
    public void batch() throws Exception {
        baki.fastInsert(DataFrame.ofRows("test.message", rows));
    }

    @Test
    public void updateFast() throws Exception {
        List<Map<String, Object>> list = Arrays.asList(
                Args.<Object>of("words", "it's my time!").add("id", 2),
                Args.<Object>of("words", "it's my time!!!").add("id", 4),
                Args.<Object>of("words", "Hello don't touch me'''").add("dt", LocalDateTime.now()).add("id", 5)
        );
        int i = baki.fastUpdate("test.message", list, "id = :id");
        System.out.println(i);
    }

    @Test
    public void update() throws Exception {
        Args<Object> args = Args.<Object>of("id", 12)
                .add("name", "chengyuxing")
                .add("words", "that's my book, don't touch!")
                .add("dt", LocalDateTime.now());

        System.out.println(SqlUtil.generateUpdate("test.message", args, "id = :id"));
    }

    @Test
    public void fors() throws Exception {
        List<String> list = Arrays.asList("a", "b", "c");
        Iterator<String> iterator = list.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            System.out.println(i);
            System.out.println(iterator.next());
        }
    }

    @Test
    public void aaa() throws Exception {
        System.out.println(new Date().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
    }
}
