package tests;

import com.github.chengyuxing.sql.util.SqlGenerator;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class SqlGeneratorRegressionTest {
    @Test
    public void backtickIdentifiersPreserveColonsAndEscapedBackticks() {
        SqlGenerator generator = new SqlGenerator(':');
        String sql = "select :id as `label:word`, `escaped``:other` from t where id = :id";
        SqlGenerator.PreparedSqlMetaData result = generator.generatePreparedSql(sql, Collections.singletonMap("id", 7));
        assertEquals("select ? as `label:word`, `escaped``:other` from t where id = ?", result.getPrepareSql());
        assertEquals(Collections.singletonMap("id", Arrays.asList(1, 2)), result.getArgNameIndexMapping());
        assertEquals("select 7 as `label:word`, `escaped``:other` from t where id = 7",
                generator.generateSql(sql, Collections.singletonMap("id", 7), Object::toString));
    }

    private final SqlGenerator generator = new SqlGenerator(':');
    private final Map<String, Integer> args = Collections.singletonMap("id", 7);

    @Test
    public void dollarQuotedLiteralsArePreserved() {
        String literals = "$$:text$$, $tag$:inside\n$$ :other $different$ 'quoted'$tag$, $标签$:name$标签$";
        SqlGenerator.PreparedSqlMetaData result = generator.generatePreparedSql("select " + literals + ", :id, :id", args);
        assertEquals("select " + literals + ", ?, ?", result.getPrepareSql());
        assertEquals(Collections.singletonMap("id", Arrays.asList(1, 2)), result.getArgNameIndexMapping());
    }

    @Test
    public void literalSqlGenerationAlsoPreservesDollarQuotes() {
        assertEquals("select $$:id$$, $body$:id$body$, 7", generator.generateSql(
                "select $$:id$$, $body$:id$body$, :id", args, String::valueOf));
    }

    @Test
    public void existingQuotingCommentsAndTypeCastsArePreserved() {
        String sql = "select ':text', \"column:other\", :id::integer -- :ignored\n/* :ignored */ where id = :id";
        SqlGenerator.PreparedSqlMetaData result = generator.generatePreparedSql(sql, args);
        assertEquals("select ':text', \"column:other\", ?::integer -- :ignored\n/* :ignored */ where id = ?", result.getPrepareSql());
        assertEquals(Collections.singletonMap("id", Arrays.asList(1, 2)), result.getArgNameIndexMapping());
    }

    @Test
    public void dollarSignsInIdentifiersDoNotStartQuotedLiterals() {
        SqlGenerator.PreparedSqlMetaData result = generator.generatePreparedSql("select column$tag$, :id, $tag$:inside$tag$", args);
        assertEquals("select column$tag$, ?, $tag$:inside$tag$", result.getPrepareSql());
        assertEquals(Collections.singletonMap("id", Collections.singletonList(1)), result.getArgNameIndexMapping());
    }
}
