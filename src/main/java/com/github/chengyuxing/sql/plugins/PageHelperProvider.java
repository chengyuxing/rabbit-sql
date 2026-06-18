package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.types.DatabaseInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Page helper provider
 */
@FunctionalInterface
public interface PageHelperProvider {
    /**
     * Implement custom page helper.
     *
     * @param databaseInfo     current database information
     * @param namedParamPrefix current named parameter prefix
     * @return PageHelper instance
     */
    @Nullable PageHelper customPageHelper(@NotNull DatabaseInfo databaseInfo, char namedParamPrefix);
}
