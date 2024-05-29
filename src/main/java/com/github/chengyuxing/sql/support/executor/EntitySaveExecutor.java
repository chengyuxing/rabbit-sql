package com.github.chengyuxing.sql.support.executor;

/**
 * Entity executor.
 *
 * @param <T> entity type
 */
public interface EntitySaveExecutor<T> {
    /**
     * Insert executor.
     *
     * @return Insert executor
     */
    SaveExecutor<T> insert();

    /**
     * Update executor.
     * <p>Generate update statement by 1st row of data, e.g.</p>
     * <p>args:</p>
     * <blockquote>
     * <pre>{id:14, name:'cyx', address:'kunming'}, {...}, ...
     *  </pre></blockquote>
     * <p>where condition：</p>
     * <blockquote>
     * <pre>id = :id</pre>
     * </blockquote>
     * <p>generated update statement：</p>
     * <blockquote>
     * <pre>update [table] set
     * name = :name,
     * address = :address
     * where id = :id</pre>
     * </blockquote>
     * Notice: where condition must contain at least 1 named parameter and args must contain its value.
     *
     * @param where condition
     * @return Update executor
     */
    SaveExecutor<T> update(String where);

    /**
     * Delete executor.
     * <p>Methods {@link SaveExecutor#safe() safe(boolean?)} and {@link SaveExecutor#ignoreNull() ignoreNull(boolean?)}
     * were not implements, ignore please.</p>
     *
     * @param where condition
     * @return Delete executor
     */
    SaveExecutor<T> delete(String where);
}
