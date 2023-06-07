package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.PagedResource;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * 分页查询构建器通用接口
 */
public abstract class IPageable {
    protected Map<String, Object> args = new HashMap<>();
    protected final String recordQuery;
    protected String countQuery;
    protected final int page;
    protected final int size;
    protected Integer count;
    protected boolean disablePageSql;
    protected Function<Map<String, Integer>, Map<String, Integer>> rewriteArgsFunc;
    protected PageHelperProvider pageHelperProvider;

    /**
     * 分页构建器构造函数
     *
     * @param recordQuery 查询sql
     * @param page        当前页
     * @param size        页大小
     */
    public IPageable(String recordQuery, int page, int size) {
        this.recordQuery = recordQuery;
        this.page = page;
        this.size = size;
    }

    /**
     * 设置sql参数
     *
     * @param args 参数
     * @return 分页对象
     */
    public IPageable args(Map<String, Object> args) {
        if (args != null) {
            this.args = args;
        }
        return this;
    }

    /**
     * 设置sql查询条数
     *
     * @param countQuery count查询语句
     * @return 分页对象
     */
    public IPageable count(String countQuery) {
        this.countQuery = countQuery;
        return this;
    }

    /**
     * 设置sql查询条数
     *
     * @param count 总数据条数
     * @return 分页对象
     */
    public IPageable count(Integer count) {
        this.count = count;
        return this;
    }

    /**
     * 禁用默认生成（{@link PageHelper#pagedSql(String)}）的分页sql，将不进行内部的分页sql构建，
     * 意味着需要自己实现个性化的分页sql
     *
     * @param countQuery count查询语句或者 {@link #count(String)}
     * @return 当前原生的查询sql
     */
    public IPageable disableDefaultPageSql(String countQuery) {
        this.disablePageSql = true;
        this.countQuery = countQuery;
        return this;
    }

    /**
     * 重写默认（{@link PageHelper#pagedArgs()}）的分页参数
     *
     * @param func 分页参数重写函数
     * @return 自定义的分页参数
     */
    public IPageable rewriteDefaultPageArgs(Function<Map<String, Integer>, Map<String, Integer>> func) {
        this.rewriteArgsFunc = func;
        return this;
    }

    /**
     * 设置当前分页查询的自定义分页提供程序
     *
     * @param pageHelperProvider 分页帮助提供程序
     * @return 分页构建器
     * @see #disableDefaultPageSql(String)
     * @see #rewriteDefaultPageArgs(Function)
     */
    public IPageable pageHelper(PageHelperProvider pageHelperProvider) {
        this.pageHelperProvider = pageHelperProvider;
        return this;
    }

    /**
     * 收集结果集操作
     *
     * @param mapper 行数据映射函数
     * @param <T>    数据类型
     * @return 已分页的资源
     */
    public abstract <T> PagedResource<T> collect(Function<DataRow, T> mapper);

    /**
     * 收集结果集操作
     *
     * @return 已分页的资源
     */
    public PagedResource<DataRow> collect() {
        return collect(d -> d);
    }
}
