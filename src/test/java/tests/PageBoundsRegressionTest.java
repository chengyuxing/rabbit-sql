package tests;

import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.page.impl.*;
import org.junit.Test;

import static org.junit.Assert.*;

public class PageBoundsRegressionTest {
    private PageHelper[] helpers() {
        return new PageHelper[]{new PGPageHelper(), new MysqlPageHelper(), new OraclePageHelper(), new Db2PageHelper(), new SqlServer2012PageHelper()};
    }

    @Test
    public void invalidPageSizeAndCountAreRejected() {
        for (PageHelper helper : helpers()) {
            assertThrows(IllegalArgumentException.class, () -> helper.init(0, 10, 1));
            assertThrows(IllegalArgumentException.class, () -> helper.init(-1, 10, 1));
            assertThrows(IllegalArgumentException.class, () -> helper.init(1, 0, 1));
            assertThrows(IllegalArgumentException.class, () -> helper.init(1, -1, 1));
            assertThrows(IllegalArgumentException.class, () -> helper.init(1, 10, -1));
            assertThrows(IllegalArgumentException.class, () -> helper.init(100000, 100000, 1));
        }
    }

    @Test
    public void validPageArgumentsArePreservedForEveryDialect() {
        PGPageHelper pg = new PGPageHelper();
        pg.init(3, 10, 25);
        assertEquals(10, pg.limit());
        assertEquals(20, pg.offset());
        assertEquals(3, pg.getPageCount());
        MysqlPageHelper mysql = new MysqlPageHelper();
        mysql.init(3, 10, 25);
        assertEquals(20, mysql.limit());
        assertEquals(10, mysql.size());
        OraclePageHelper oracle = new OraclePageHelper();
        oracle.init(3, 10, 25);
        assertEquals(21, oracle.start());
        assertEquals(25, oracle.end());
    }

    @Test
    public void oracleEndIsClampedBeforeNarrowingToInteger() {
        OraclePageHelper oracle = new OraclePageHelper();
        oracle.init(2, 1073741824, Integer.MAX_VALUE);
        assertEquals(1073741825, oracle.start());
        assertEquals(Integer.MAX_VALUE, oracle.end());
    }

    @Test
    public void oracleOneBasedStartRejectsOverflowAtIntegerBoundary() {
        OraclePageHelper oracle = new OraclePageHelper();
        oracle.init(2, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, oracle::start);
        PGPageHelper pg = new PGPageHelper();
        pg.init(2, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, pg.offset());
    }

    @Test
    public void invalidReinitializationDoesNotChangeExistingPage() {
        PGPageHelper page = new PGPageHelper();
        page.init(3, 10, 25);
        assertThrows(IllegalArgumentException.class, () -> page.init(0, 0, -1));
        assertEquals(3, page.getPageNumber());
        assertEquals(10, page.getPageSize());
        assertEquals(25, page.getRecordCount());
    }

    @Test
    public void emptyPagedResourcesStillValidateInputs() {
        assertThrows(IllegalArgumentException.class, () -> PagedResource.empty(0, 10));
        assertThrows(IllegalArgumentException.class, () -> PagedResource.empty(1, 0));
        PagedResource<Object> resource = PagedResource.empty(1, 10);
        assertTrue(resource.getData().isEmpty());
        assertEquals(0, resource.getPager().getPageCount());
    }
}
