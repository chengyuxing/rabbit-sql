package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.page.impl.MysqlPageHelper;
import com.github.chengyuxing.sql.page.impl.OraclePageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * 分页查询构建器通用接口
 *
 * @param <T> 结果类型参数
 */
public abstract class IPageable<T> {
    protected Map<String, Object> args = new HashMap<>();
    protected final String recordQuery;
    protected String countQuery;
    protected final int page;
    protected final int size;
    protected Integer count;
    protected PageHelper customPageHelper;

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
    public IPageable<T> args(Map<String, Object> args) {
        this.args = args;
        return this;
    }

    /**
     * 设置sql查询条数
     *
     * @param countQuery count查询语句
     * @return 分页对象
     */
    public IPageable<T> count(String countQuery) {
        this.countQuery = countQuery;
        return this;
    }

    /**
     * 设置sql查询条数
     *
     * @param count 总数据条数
     * @return 分页对象
     */
    public IPageable<T> count(Integer count) {
        this.count = count;
        return this;
    }

    /**
     * 设置自定义的分页帮助工具类
     *
     * @param pageHelper 分页帮助类
     * @return 分页构建器
     * @see OraclePageHelper
     * @see PGPageHelper
     * @see MysqlPageHelper
     */
    public IPageable<T> pageHelper(PageHelper pageHelper) {
        this.customPageHelper = pageHelper;
        return this;
    }

    /**
     * 收集结果集操作
     *
     * @param mapper 行数据映射函数
     * @return 已分页的资源
     */
    public abstract PagedResource<T> collect(Function<DataRow, T> mapper);
}
