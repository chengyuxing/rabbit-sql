package baki.entityExecutor;

@FunctionalInterface
public interface LazyReference<T> {
    T get();
}
