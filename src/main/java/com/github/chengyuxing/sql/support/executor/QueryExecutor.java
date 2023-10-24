package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.IPageable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * 查询执行器
 */
public abstract class QueryExecutor {
    protected final String sql;
    protected Map<String, Object> args = new HashMap<>();

    /**
     * 构造函数
     *
     * @param sql sql或sql名
     */
    public QueryExecutor(String sql) {
        this.sql = sql;
    }

    /**
     * 设置sql中的参数字典
     *
     * @param args 参数字典
     * @return 查询执行器
     * @see com.github.chengyuxing.sql.Args Args&lt;Object&gt;
     */
    public QueryExecutor args(Map<String, Object> args) {
        if (args != null) {
            this.args = new HashMap<>(args);
        }
        return this;
    }

    /**
     * 设置sql中的参数字典
     *
     * @param keyValues 多组 key-value 结构参数
     * @return 查询执行器
     */
    public QueryExecutor args(Object... keyValues) {
        if (keyValues.length > 0) {
            this.args = Args.of(keyValues);
        }
        return this;
    }

    /**
     * 添加sql中的参数
     *
     * @param key   键
     * @param value 值
     * @return 查询执行器
     */
    public QueryExecutor arg(String key, Object value) {
        this.args.put(key, value);
        return this;
    }

    /**
     * 查询为一个流对象<br>
     * 一个流对象占用一个连接对象，需要手动关闭流或使用try-with-resource包裹
     *
     * @return 收集为流的结果集
     * @see com.github.chengyuxing.sql.support.JdbcSupport#executeQueryStream(String, Map) executeQueryStream(String, Map)
     */
    public abstract Stream<DataRow> stream();

    /**
     * 查询为一组map对象
     *
     * @return 一组map对象
     */
    public abstract List<Map<String, Object>> maps();

    /**
     * 查询为一组DataRow对象
     *
     * @return 一组DataRow对象
     */
    public abstract List<DataRow> rows();

    /**
     * 查询为一组标准java bean实体
     *
     * @param entityClass 实体类
     * @param <T>         实体类型
     * @return 一组实体
     */
    public abstract <T> List<T> entities(Class<T> entityClass);

    /**
     * 分页查询<br>
     * 可能默认的构建分页SQL无法满足所有情况，例如PostgreSQL中:
     * <pre>with a as (select ... limit 0 offset 5)<br>select * from a;</pre>
     * 关于自定义分页SQL配置如下:
     * <blockquote>
     * <ul>
     * <li>禁用自动分页构建：{@link IPageable#disableDefaultPageSql(String) disableDefaultPageSql(String)}，如果不禁用，如上例子会在SQL结尾加{@code limit ... offset ...}</li>
     * <li>自定义count查询语句：{@link IPageable#count(String) count(String)}</li>
     * <li>如有必要自定义个性化参数名：{@link IPageable#rewriteDefaultPageArgs(Function) rewriteDefaultPageArgs(Function)}</li>
     * </ul>
     * </blockquote>
     *
     * @param page 页码
     * @param size 每页大小
     * @return 分页构建器
     */
    public abstract IPageable pageable(int page, int size);

    /**
     * 分页查询一条记录
     *
     * @return 一条记录或空行
     * @see #findFirst()
     */
    public abstract DataRow findFirstRow();

    /**
     * 分页查询一个实体
     *
     * @param entityClass 实体类
     * @param <T>         实体类型
     * @return 一个实体对象或null
     * @see #findFirst()
     */
    public abstract <T> T findFirstEntity(Class<T> entityClass);

    /**
     * 分页查询一条记录
     * <p>注意和 {@link  QueryExecutor#stream() stream().findFirst()} 的本质区别在于：</p>
     * <blockquote>
     *     <ul>
     *         <li>{@link  QueryExecutor#stream() stream().findFirst()} ：执行原始sql，返回第1条记录（如果表数据量大且查询没做合理的条件限制，即使内部仅迭代一次，性能也会下降）；</li>
     *         <li>{@code findFirst()} ：对sql进行分页处理，真实进行分页查询1条记录；</li>
     *     </ul>
     * </blockquote>
     * 例如 Postgresql：
     * <blockquote>
     * <pre>source: select * from users</pre>
     * <pre>target: select * from users limit 1 offset 0</pre>
     * </blockquote>
     *
     * @return 可为空的一条记录
     */
    public abstract Optional<DataRow> findFirst();

    /**
     * 查询结果是否存在
     *
     * @return 是否存在
     */
    public abstract boolean exists();
}
