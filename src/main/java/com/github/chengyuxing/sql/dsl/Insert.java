package com.github.chengyuxing.sql.dsl;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;

public abstract class Insert<T> {
    public abstract int saveEntity(@NotNull T entity);

    public abstract int saveEntities(@NotNull Collection<T> entities);

    public abstract int saveMap(@NotNull Map<String, Object> data);

    public abstract int saveMaps(@NotNull Collection<Map<String, Object>> data);
}
