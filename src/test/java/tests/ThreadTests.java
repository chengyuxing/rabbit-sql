package tests;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ThreadTests {
    static class User {
        private static User instance = null;

        public void hello() {}

        public static User getInstance() {
            if (instance == null) {
                instance = new User();
                System.out.println("new!");
            }
            return instance;
        }
    }

    @Test
    public void test1() {
        for (int i = 0; i < 5; i++) {
            new Thread(() -> User.getInstance().hello()).start();
        }
    }
}
