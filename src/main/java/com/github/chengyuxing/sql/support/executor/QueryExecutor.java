package com.github.chengyuxing.sql.support.executor;

import com.github.chengyuxing.common.DataRow;
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
    protected QueryExecutor(String sql) {
        this.sql = sql;
    }

    /**
     * 设置sql中的参数字典
     *
     * @param args 参数字典
     * @return 查询执行器
     */
    public QueryExecutor args(Map<String, Object> args) {
        if (!this.args.isEmpty()) {
            this.args.putAll(args);
        } else {
            this.args = args;
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
     * 分页查询<br>
     * 可能默认的构建分页SQL无法满足所有情况，例如PostgreSQL中:
     * <pre>with a as (select ... limit 0 offset 5)<br>select * from a;</pre>
     * 关于自定义分页SQL配置如下:
     * <blockquote>
     * <ul>
     * <li>禁用自动分页构建：{@link IPageable#disableDefaultPageSql(String...)}，因为如上例子，否则会在SQL结尾加{@code limit ... offset ...}</li>
     * <li>自定义count查询语句：{@link IPageable#count(String)}</li>
     * <li>如有必要自定义个性化参数名：{@link IPageable#rewriteDefaultPageArgs(Function)}</li>
     * </ul>
     * </blockquote>
     *
     * @param page 页码
     * @param size 每页大小
     * @param <T>  结果类型参数
     * @return 分页构建器
     */
    public abstract <T> IPageable<T> pageable(int page, int size);

    /**
     * 查询一条记录
     *
     * @return 一条记录
     */
    public abstract DataRow findFirstRow();

    /**
     * 查询一条记录
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
