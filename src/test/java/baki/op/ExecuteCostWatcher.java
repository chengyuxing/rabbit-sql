package baki.op;

import com.github.chengyuxing.common.AroundExecutor;
import com.github.chengyuxing.sql.types.ExecutionContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteCostWatcher extends AroundExecutor<ExecutionContext> {
    private static final Logger log = LoggerFactory.getLogger(ExecuteCostWatcher.class);

    @Override
    public void before(@NotNull ExecutionContext context) {
        context.setState("startTime", System.currentTimeMillis());
    }

    @Override
    public void after(@NotNull ExecutionContext context, @Nullable Throwable throwable) {
        long startTime = context.getState("startTime");
        long cost = System.currentTimeMillis() - startTime;
        log.info("{}: {}, SPENT: {} sec.", "SQL Watcher", context.getSql(), cost / 1000.0);
    }
}
