package tests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class CleanRepositories {
    public static void main(String[] args) throws IOException {
        try (Stream<Path> paths = Files.find(Paths.get("/my/repository"), 50, (p, a) -> p.getFileName().toString().equals("_remote.repositories")
                || p.getFileName().toString().endsWith(".sha1"))) {
            paths.forEach(p -> {
                try {
                    Files.delete(p);
                    System.out.println("Delete: " + p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
