package rabbit.sql.dao;

import rabbit.common.types.DataRow;
import rabbit.sql.exceptions.ConnectionStatusException;
import rabbit.sql.page.IPageable;
import rabbit.sql.page.PageHelper;
import rabbit.sql.page.PagedResource;
import rabbit.sql.page.impl.MysqlPageHelper;
import rabbit.sql.page.impl.OraclePageHelper;
import rabbit.sql.page.impl.PGPageHelper;
import rabbit.sql.utils.SqlUtil;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 分页查询构建器
 *
 * @param <T> 结果类型参数
 */
public class Pageable<T> implements IPageable<T> {
    private final BakiDao baki;
    private Map<String, Object> args = Collections.emptyMap();
    private final String recordQuery;
    private String countQuery;
    private final int page;
    private final int size;
    private Integer count;
    private PageHelper customPageHelper;

    /**
     * 分页构建器构造函数
     *
     * @param baki        baki对象
     * @param recordQuery 查询sql
     * @param page        当前页
     * @param size        页大小
     */
    public Pageable(BakiDao baki, String recordQuery, int page, int size) {
        this.baki = baki;
        this.recordQuery = recordQuery;
        this.page = page;
        this.size = size;
    }

    @Override
    public IPageable<T> args(Map<String, Object> args) {
        this.args = args;
        return this;
    }

    @Override
    public IPageable<T> countQuery(String countQuery) {
        this.countQuery = countQuery;
        return this;
    }

    @Override
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
    @Override
    public IPageable<T> pageHelper(PageHelper pageHelper) {
        this.customPageHelper = pageHelper;
        return this;
    }

    /**
     * 收集结果集操作
     *
     * @return 已分页的资源
     * @throws rabbit.sql.exceptions.SqlRuntimeException sql执行过程中出现错误或读取结果集是出现错误
     * @throws UnsupportedOperationException             如果没有自定分页，而默认分页不支持当前数据库
     * @throws ConnectionStatusException                 如果连接对象异常
     */
    @Override
    public PagedResource<T> collect(Function<DataRow, T> rowConvert) {
        String query = baki.prepareSql(recordQuery, args);
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
        try (Stream<DataRow> s = baki.query(pageHelper.wrapPagedSql(query), args)) {
            List<T> list = s.map(rowConvert).collect(Collectors.toList());
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
    private PageHelper defaultPager() {
        try {
            String dbName = baki.getMetaData().getDatabaseProductName().toLowerCase();
            switch (dbName) {
                case "oracle":
                    return new OraclePageHelper();
                case "postgresql":
                case "sqlite":
                    return new PGPageHelper();
                case "mysql":
                    return new MysqlPageHelper();
                default:
                    throw new UnsupportedOperationException("pager of \"" + dbName + "\" not support currently!");
            }
        } catch (SQLException e) {
            throw new ConnectionStatusException("get db metadata error: ", e);
        }
    }
}
