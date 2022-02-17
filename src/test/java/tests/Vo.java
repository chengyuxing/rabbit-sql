package tests;

public class Vo {
    private volatile boolean inited = false;

    public void console() {
        if (!inited) {
            inited = true;
            System.out.println("***");
        }
    }
}
