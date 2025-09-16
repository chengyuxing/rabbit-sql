package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.types.Execution;
import org.jetbrains.annotations.Nullable;

/**
 * SQL execution watcher.
 */
public interface ExecutionWatcher {
    /**
     * On start.
     *
     * @param execution current execution
     */
    void onStart(Execution execution);

    /**
     * On stop.
     *
     * @param execution current execution
     * @param result    execute result
     * @param throwable exception
     */
    void onStop(Execution execution, @Nullable Object result, @Nullable Throwable throwable);
}
