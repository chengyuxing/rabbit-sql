package rabbit.sql;

import rabbit.common.types.DataRow;
import rabbit.sql.page.AbstractPageHelper;
import rabbit.sql.page.Pageable;
import rabbit.sql.support.ICondition;
import rabbit.sql.types.Ignore;
import rabbit.sql.types.Param;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * dao接口
 */
public interface Light {

    /**
     * 执行一条原始sql
     *
     * @param sql 原始sql
     * @return 如果执行成功，DML语句返回1，DDL语句返回0
     */
    long execute(String sql);

    /**
     * 执行一条原始sql
     *
     * @param sql    原始sql
     * @param params 参数
     * @return 如果执行成功，DML语句返回1，DDL语句返回0
     */
    long execute(String sql, Map<String, Param> params);

    /**
     * 插入
     *
     * @param tableName 表名
     * @param data      数据
     * @return 受影响的行数
     */
    int insert(String tableName, Map<String, Param> data);

    /**
     * 插入
     *
     * @param tableName 表名
     * @param data      数据
     * @param ignore    忽略插入的值类型
     * @return 受影响的行数
     */
    int insert(String tableName, Map<String, Param> data, Ignore ignore);

    /**
     * 插入
     *
     * @param tableName 表名
     * @param row       数据
     * @return 受影响的行数
     */
    int insert(String tableName, DataRow row);

    /**
     * 插入
     *
     * @param tableName 表名
     * @param row       数据
     * @param ignore    忽略插入的值类型
     * @return 受影响的行数
     */
    int insert(String tableName, DataRow row, Ignore ignore);

    /**
     * 批量插入
     *
     * @param tableName 表名
     * @param data      数据
     * @return 受影响的行数
     */
    int insert(String tableName, Collection<Map<String, Param>> data);

    /**
     * 删除
     *
     * @param tableName  表名
     * @param ICondition 条件
     * @return 受影响的行数
     */
    int delete(String tableName, ICondition ICondition);

    /**
     * 更新
     *
     * @param tableName  表名
     * @param data       数据
     * @param ICondition 条件
     * @return 受影响的行数
     */
    int update(String tableName, Map<String, Param> data, ICondition ICondition);

    /**
     * 查询<br>
     *
     * @param sql 查询sql
     * @return 收集为流的结果集
     */
    Stream<DataRow> query(String sql);

    /**
     * 查询<br>
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 收集为流的结果集
     */
    Stream<DataRow> query(String sql, Map<String, Param> args);

    /**
     * 查询<br>
     *
     * @param sql        查询sql
     * @param ICondition 参数
     * @return 收集为流的结果集
     */
    Stream<DataRow> query(String sql, ICondition ICondition);

    /**
     * 分页查询
     *
     * @param recordQuery 查询SQL
     * @param countQuery  查询记录数SQL
     * @param convert     行转换
     * @param args        参数
     * @param page        分页对象
     * @param <T>         目标类型
     * @return 分页的结果集
     */
    <T> Pageable<T> query(String recordQuery, String countQuery, Function<DataRow, T> convert, Map<String, Param> args, AbstractPageHelper page);

    /**
     * 分页查询
     *
     * @param recordQuery 查询SQL
     * @param countQuery  查询记录数SQL
     * @param convert     行转换
     * @param ICondition  条件拼接器
     * @param page        分页对象
     * @param <T>         目标类型
     * @return 分页的结果集
     */
    <T> Pageable<T> query(String recordQuery, String countQuery, Function<DataRow, T> convert, ICondition ICondition, AbstractPageHelper page);

    /**
     * 获取一条<br>
     *
     * @param sql 查询sql
     * @return 空或一条
     */
    Optional<DataRow> fetch(String sql);

    /**
     * 获取一条<br>
     *
     * @param sql        查询sql
     * @param ICondition 条件
     * @return 空或一条
     */
    Optional<DataRow> fetch(String sql, ICondition ICondition);

    /**
     * 获取一条<br>
     *
     * @param sql  查询sql
     * @param args 参数
     * @return 空或一条
     */
    Optional<DataRow> fetch(String sql, Map<String, Param> args);

    /**
     * 判断是否存在数据行
     *
     * @param sql sql
     * @return 是否存在
     */
    boolean exists(String sql);

    /**
     * 判断是否存在数据行
     *
     * @param sql        sql
     * @param ICondition 条件
     * @return 是否存在
     */
    boolean exists(String sql, ICondition ICondition);

    /**
     * 判断是否存在数据行
     *
     * @param sql  sql
     * @param args 参数
     * @return 是否存在
     */
    boolean exists(String sql, Map<String, Param> args);

    /**
     * 执行存储过程
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return 一个或多个结果或无结果
     */
    DataRow procedure(String name, Map<String, Param> args);

    /**
     * 执行函数
     *
     * @param name 函数名
     * @param args 参数（占位符名字，参数对象）
     * @return 至少一个结果
     */
    DataRow function(String name, Map<String, Param> args);

    /**
     * 获取数据库的元数据信息
     *
     * @return 数据库的元数据信息
     * @throws SQLException sql连接异常
     */
    DatabaseMetaData getMetaData() throws SQLException;
}
