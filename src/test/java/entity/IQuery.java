package entity;

import com.github.chengyuxing.sql.page.IPageable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface IQuery<T> {
    IQuery<T> fields(FieldFunction<T>... fields);

    IQuery<T> orderByAsc(FieldFunction<T> field);

    IQuery<T> orderByDesc(FieldFunction<T> field);

    IQuery<T> where(FieldFunction<T> field, String op, Object value);

    IQuery<T> or(FieldFunction<T> field, String op, Object value);

    IQuery<T> and(FieldFunction<T> field, String op, Object value);

    IQuery<T> limit(int num);

    Stream<T> stream();

    List<T> list();

    T first();

    Optional<T> findFirst();

    default boolean exists() {
        return findFirst().isPresent();
    }

    int count();

    IPageable pageable(int page, int size);
}
