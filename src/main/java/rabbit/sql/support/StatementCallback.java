package rabbit.sql.support;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 执行声明回调函数，通过这个特殊的函数可以在一个执行器中完成所有操作
 *
 * @param <T>返回类型参数
 */
@FunctionalInterface
public interface StatementCallback<T> {
    /**
     * 声明对象中执行一个操作
     *
     * @param statement statement
     * @return 任何想返回的类型
     * @throws SQLException sqlExp
     */
    T doInStatement(PreparedStatement statement) throws SQLException;
}
