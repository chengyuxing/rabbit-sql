package sql;

import com.github.chengyuxing.common.script.impl.FastExpression;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlTests {
    @Test
    public void test1() throws Exception {
        String sqlPath = "&get/User/By/Id(id,name,age)";
        String sqlName = sqlPath.substring(1);

    }

    static final Pattern FOR = Pattern.compile("(?<item>[^,\\s]+)(,(?<index>\\S+))?\\s+of\\s+:(?<list>\\S+)((\\s+)delimiter\\s+'(?<delimiter>[^']+)')?(\\s+filter\\s+(?<filter>[\\S\\s]+))?", Pattern.MULTILINE);

    @Test
    public void testFor() throws Exception {
        List<Map<String, Object>> list = Arrays.asList(Args.of("name", "cyx"), Args.of("name", "jackson"));
        String s = "--#for item,index of :list\n" +
                "and name = ${item.name}\n" +
                "--#end";
        Matcher m = FOR.matcher(s);
        if (m.find()) {
            System.out.println(m.group("item"));
            System.out.println(m.group("index"));
            System.out.println(m.group("list"));
            System.out.println(m.group("delimiter"));
        }
        String itemName = m.group("item");
        String idxName = m.group("index");
        String listName = m.group("list");
        String delimiter = m.group("delimiter");
        StringJoiner sj = new StringJoiner(delimiter == null ? ", " : delimiter);

        Pattern itemP = Pattern.compile("\\$\\{(?<tmp>" + itemName + "(.\\w+)*)}");
        for (int i = 0; i < list.size(); i++) {
            String sqlPart = "and name = ${item.name} and name id = ${index}";
            Matcher mx = itemP.matcher(sqlPart);
            while (mx.find()) {
                String valuePath = mx.group("tmp").substring(itemName.length());
                String jPath = valuePath.replace(".", "/");
                Object value = ObjectUtil.getDeepNestValue(list.get(i), jPath);
                sqlPart = sqlPart.replace("${" + itemName + valuePath + "}", SqlUtil.quoteFormatValueIfNecessary(value));
            }
            if (idxName != null) {
                sqlPart = sqlPart.replace("${" + idxName + "}", i + "");
            }
            sj.add(sqlPart);
        }
        System.out.println(sj);
    }

    @Test
    public void testForLoop() throws Exception {
        List<Map<String, Object>> list = Arrays.asList(Args.of("name", "cyx"), Args.of("name", "jackson"), Args.of("name", null));
        String s = "and name in (\n" +
                "--#for item,index of :list delimiter '\\n' filter ${item.name} ~ 'j'\n" +
                "${item.name}\n" +
                "--#end\n" +
                ")\n";
        doFor(s, list);
    }

    public void doFor(String sql, List<Map<String, Object>> list) throws NoSuchFieldException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        String[] sqls = sql.split("\n");
        for (int i = 0; i < sqls.length; i++) {
            String line = sqls[i];
            if (line.startsWith("--#for")) {
                Matcher m = FOR.matcher(line.substring(6));
                if (m.find()) {
                    String itemName = m.group("item");
                    String idxName = m.group("index");
                    String listName = m.group("list");
                    String delimiter = m.group("delimiter");
                    String filter = m.group("filter");
                    StringJoiner loopPart = new StringJoiner("\n");

                    while (!sqls[++i].startsWith("--#end")) {
                        loopPart.add(sqls[i]);
                    }

                    StringJoiner forSql = new StringJoiner(delimiter == null ? ", " : delimiter.replace("\\n", "\n").replace("\\t", "\t"));

                    Pattern itemP = Pattern.compile("\\$\\{(?<tmp>" + itemName + "(.\\S+)*)}");
                    for (int j = 0; j < list.size(); j++) {

                        Matcher vx = itemP.matcher(filter);
                        Map<String, Object> fArgs = new HashMap<>();
                        fArgs.put(idxName, j);
                        String expStr = filter;
                        while (vx.find()) {
                            String valuePath = vx.group("tmp").substring(itemName.length());
                            String jPath = valuePath.replace(".", "/");
                            Object value = ObjectUtil.getDeepNestValue(list.get(j), jPath);
                            fArgs.put(vx.group("tmp"), value);
                            expStr = expStr.replace("${" + itemName + valuePath + "}", ":" + itemName + valuePath);
                        }

                        FastExpression expression = FastExpression.of(expStr);
                        if (expression.calc(fArgs)) {
                            String sqlPart = loopPart.toString();
                            Matcher mx = itemP.matcher(sqlPart);
                            while (mx.find()) {
                                String valuePath = mx.group("tmp").substring(itemName.length());
                                String jPath = valuePath.replace(".", "/");
                                Object value = ObjectUtil.getDeepNestValue(list.get(j), jPath);
                                sqlPart = sqlPart.replace("${" + itemName + valuePath + "}", SqlUtil.quoteFormatValueIfNecessary(value));
                            }
                            if (idxName != null) {
                                sqlPart = sqlPart.replace("${" + idxName + "}", j + "");
                            }
                            forSql.add(sqlPart);
                        }
                    }
                    String xql = forSql.toString();
                    System.out.println(xql);
                }
            }
        }
    }

    @Test
    public void testz() throws Exception{
        System.out.println(SqlUtil.quoteFormatValueIfNecessary(new Object[]{"a","'v","D"}));
    }
}
