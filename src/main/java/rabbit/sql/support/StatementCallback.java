package rabbit.sql.support;

import java.sql.CallableStatement;
import java.sql.SQLException;

public interface StatementCallback<T> {
    T doInStatement(CallableStatement statement) throws SQLException;

    String getExecuteSql();
}
