package tests;

public class CurrentTests {
    public static void main(String[] args) {
        Vo vo = new Vo();
        for (int i = 0; i < 5; i++) {
            new Thread(vo::console).start();
        }
    }
}
