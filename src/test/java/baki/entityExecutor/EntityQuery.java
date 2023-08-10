package baki.entityExecutor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class EntityQuery<T> {
    public abstract Stream<T> stream();

    public abstract List<T> list();

    public abstract Optional<T> findFirst();
}
