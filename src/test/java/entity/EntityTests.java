package entity;

import com.github.chengyuxing.sql.BakiDao;
import org.junit.Test;
import tests.User;

import java.util.stream.Stream;

public class EntityTests {
    @Test
    public void test1() {
        EntityManager<User> entityManager = new EntityManager<>(BakiDao.of(null), User.class);

//        Stream<User> userStream = entityManager.query(User.class)
//                .fields(User::getName, User::getAge)
//                .where(User::getPassword, "=", "123456")
//                .and(User::getAge, ">", 29)
//                .orderByDesc(User::getAge)
//                .orderByDesc(User::getName)
//                .stream();
    }
}
