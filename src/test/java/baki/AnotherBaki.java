package baki;

import com.github.chengyuxing.sql.BakiDao;

import javax.sql.DataSource;

public class AnotherBaki extends BakiDao {
    /**
     * 构造函数
     *
     * @param dataSource 数据源
     */
    public AnotherBaki(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected void setDataSource(DataSource dataSource) {
        super.setDataSource(dataSource);
    }
}
