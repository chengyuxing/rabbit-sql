package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.types.DatabaseInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.util.function.Supplier;

/**
 * Database info provider.
 */
public interface DatabaseInfoProvider {
    /**
     * To returns a database info by of current database.
     * <p>
     * This is mainly in terms of dynamic datasource, check the datasource unique key, to returns a correct database info.
     * <p>
     * Avoid to create new connection each time, it's better and necessary to cache the database info by unique key e.g.
     * <blockquote><pre>
     *     if(dataSource instanceof DynamicDataSource){
     *         String key = DynamicDataSourceContextHolder.peek();
     *         return INFO.computeIfAbsent(key, k -> supplier.get());
     *     }
     * </pre></blockquote>
     * Warning: Do not close the datasource!
     *
     * @param dataSource current target datasource
     * @param supplier   fetch a new auto-release connection to create a database info object
     * @return current database info object or null
     */
    @Nullable DatabaseInfo resolve(@NotNull DataSource dataSource, Supplier<DatabaseInfo> supplier);
}
