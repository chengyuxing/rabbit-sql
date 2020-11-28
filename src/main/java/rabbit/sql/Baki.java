package rabbit.sql;

import rabbit.common.types.DataRow;
import rabbit.sql.page.IPageable;
import rabbit.sql.support.ICondition;
import rabbit.sql.types.DataFrame;
import rabbit.sql.types.Param;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * dao接口
 */
public interface Baki {

    /**
     * 执行一条原始sql
     *
     * @param sql 原始sql
     * @return 如果执行成功，DML语句返回1，DDL语句返回0
     */
    DataRow execute(String sql);

    /**
     * 执行一条原始sql
     *
     * @param sql  原始sql
     * @param args 参数
     * @return 如果执行成功，DML语句返回1，DDL语句返回0
     */
    DataRow execute(String sql, Map<String, Object> args);

    /**
     * 插入
     *
     * @param dataFrame 表名
     * @return 受影响的行数
     */
    int insert(DataFrame dataFrame);

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
    int update(String tableName, Map<String, Object> data, ICondition ICondition);

    /**
     * 查询
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
    Stream<DataRow> query(String sql, Map<String, Object> args);

    /**
     * 分页查询
     *
     * @param query 查询sql
     * @param page  当前页
     * @param size  分页大小
     * @param <T>   类型参数
     * @return 分页构建器
     */
    <T> IPageable<T> query(String query, int page, int size);

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
     * @param sql  查询sql
     * @param args 参数
     * @return 空或一条
     */
    Optional<DataRow> fetch(String sql, Map<String, Object> args);

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
     * @param sql  sql
     * @param args 参数
     * @return 是否存在
     */
    boolean exists(String sql, Map<String, Object> args);

    /**
     * 执行存储过程或函数
     *
     * @param name 过程名
     * @param args 参数 （占位符名字，参数对象）
     * @return 一个或多个结果或无结果
     */
    DataRow call(String name, Map<String, Param> args);

    /**
     * 获取数据库的元数据信息
     *
     * @return 数据库的元数据信息
     * @throws SQLException sql连接异常
     */
    DatabaseMetaData getMetaData() throws SQLException;
}
