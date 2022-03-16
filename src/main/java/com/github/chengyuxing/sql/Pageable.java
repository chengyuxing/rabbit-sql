package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.exceptions.ConnectionStatusException;
import com.github.chengyuxing.sql.exceptions.UncheckedSqlException;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.MysqlPageHelper;
import com.github.chengyuxing.sql.page.impl.OraclePageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;
import com.github.chengyuxing.sql.utils.SqlUtil;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 分页查询构建器
 *
 * @param <T> 结果类型参数
 */
public class Pageable<T> extends IPageable<T> {
    private final BakiDao baki;

    /**
     * 分页构建器构造函数
     *
     * @param baki        baki对象
     * @param recordQuery 查询sql
     * @param page        当前页
     * @param size        页大小
     */
    public Pageable(BakiDao baki, String recordQuery, int page, int size) {
        super(recordQuery, page, size);
        this.baki = baki;
    }

    /**
     * {@inheritDoc}
     *
     * @param mapper 行数据映射函数
     * @return 分页数据
     * @throws UncheckedSqlException           sql执行过程中出现错误或读取结果集是出现错误
     * @throws UnsupportedOperationException 如果没有自定分页，而默认分页不支持当前数据库
     * @throws ConnectionStatusException     如果连接对象异常
     */
    @Override
    public PagedResource<T> collect(Function<DataRow, T> mapper) {
        String query = baki.getSql(recordQuery, args);
        if (count == null) {
            String cq = countQuery;
            if (cq == null) {
                cq = SqlUtil.generateCountQuery(query);
            }
            count = baki.fetch(cq, args).map(cn -> cn.getInt(0)).orElse(0);
        }
        PageHelper pageHelper = customPageHelper;
        if (pageHelper == null) {
            pageHelper = defaultPager();
        }
        pageHelper.init(page, size, count);
        args.putAll(pageHelper.pagedArgs());
        try (Stream<DataRow> s = baki.query(pageHelper.pagedSql(query), args)) {
            List<T> list = s.map(mapper).collect(Collectors.toList());
            return PagedResource.of(pageHelper, list);
        }
    }

    /**
     * 根据数据库名字自动选择合适的默认分页帮助类
     *
     * @return 分页帮助类
     * @throws UnsupportedOperationException 如果没有自定分页，而默认分页不支持当前数据库
     * @throws ConnectionStatusException     如果连接对象异常
     */
    protected PageHelper defaultPager() {
        try {
            String dbName = baki.metaData().getDatabaseProductName().toLowerCase();
            switch (dbName) {
                case "oracle":
                    return new OraclePageHelper();
                case "postgresql":
                case "sqlite":
                    return new PGPageHelper();
                case "mysql":
                    return new MysqlPageHelper();
                default:
                    throw new UnsupportedOperationException("pager of \"" + dbName + "\" not support currently, see method 'pageHelper'!");
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException("get database metadata error: ", e);
        }
    }
}
