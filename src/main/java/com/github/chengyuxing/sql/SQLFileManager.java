package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.script.impl.FastExpression;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.exceptions.IORuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.utils.ResourceUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.utils.SqlUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.containsAllIgnoreCase;
import static com.github.chengyuxing.common.utils.StringUtil.startsWithIgnoreCase;
import static com.github.chengyuxing.sql.utils.SqlUtil.removeAnnotationBlock;

/**
 * SQL文件解析管理器<br>
 * 支持外部sql(本地文件系统)和classpath下的sql<br>
 * 本地sql文件以 {@code file:} 开头，默认读取classpath下的sql文件<br>
 * e.g. SQL文件格式
 * <blockquote>
 * <pre>windows: file:\\D:\\rabbit.sql</pre>
 * <pre>Linux/Unix: file:/root/rabbit.sql</pre>
 * <pre>ClassPath: sql/rabbit.sql</pre>
 * </blockquote>
 * 格式参考data.sql.template
 */
public class SQLFileManager {
    private final static Logger log = LoggerFactory.getLogger(SQLFileManager.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, String> RESOURCE = new HashMap<>();
    private final AtomicInteger UN_NAMED_SQL_INDEX = new AtomicInteger();
    private final String UN_NAMED_SQL_NAME = "UN_NAMED_SQL_";
    private final Map<String, Long> LAST_MODIFIED = new HashMap<>();
    private static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    private static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");
    public static final String IF = "--#if";
    public static final String FI = "--#fi";
    public static final String CHOOSE = "--#choose";
    public static final String END = "--#end";
    // ----------------optional properties------------------
    private volatile boolean checkModified;
    private Map<String, String> constants = new HashMap<>();
    private String[] sqls;
    private Map<String, String> sqlMap;
    private List<String> sqlList;

    public SQLFileManager() {
    }

    /**
     * Sql文件解析器实例<br>
     *
     * @param sqls 多个文件名，以逗号(,)分割
     */
    public SQLFileManager(String sqls) {
        this.sqls = sqls.split(",");
    }

    /**
     * 添加命名的sql文件
     *
     * @param alias       sql文件别名
     * @param sqlFileName sql文件全路径名
     */
    public void add(String alias, String sqlFileName) {
        if (sqlMap == null) {
            sqlMap = new HashMap<>();
        }
        sqlMap.put(alias, sqlFileName);
    }

    /**
     * 设置命名的sql文件
     *
     * @param sqlMap 命名sql文件名和路径对应关系
     */
    public void setSqlMap(Map<String, String> sqlMap) {
        this.sqlMap = sqlMap;
    }

    /**
     * 添加未命名的sql文件
     *
     * @param sqlFileName sql文件全路径名
     */
    public void add(String sqlFileName) {
        if (sqlList == null) {
            sqlList = new ArrayList<>();
        }
        sqlList.add(sqlFileName);
    }

    /**
     * 设置未命名的sql文件
     *
     * @param sqlList sql文件全路径列表
     */
    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }

    /**
     * 解析sql文件
     *
     * @param name     sql文件自定义名
     * @param resource 类路径sql资源
     * @throws IOException        如果sql文件不存在或路径无效
     * @throws DuplicateException 如果同一个sql文件中有重复的sql名
     */
    private void resolveSqlContent(String name, FileResource resource) throws IOException {
        Map<String, String> singleResource = new HashMap<>();
        String pkgPath = ResourceUtil.path2package(resource.getPath());
        String prefix = pkgPath.substring(0, pkgPath.length() - 3);
        if (!name.startsWith(UN_NAMED_SQL_NAME)) {
            prefix = name + ".";
        }
        String previousSqlName = "";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (!trimLine.isEmpty()) {
                    Matcher m_name = NAME_PATTERN.matcher(trimLine);
                    Matcher m_part = PART_PATTERN.matcher(trimLine);
                    if (m_name.matches()) {
                        previousSqlName = prefix + m_name.group("name");
                        if (singleResource.containsKey(previousSqlName)) {
                            throw new DuplicateException("same sql name: " + previousSqlName);
                        }
                        singleResource.put(previousSqlName, "");
                    } else if (m_part.matches()) {
                        previousSqlName = "${" + prefix + m_part.group("part") + "}";
                        if (singleResource.containsKey(previousSqlName)) {
                            throw new DuplicateException("same sql part: " + previousSqlName);
                        }
                        singleResource.put(previousSqlName, "");
                    } else {
                        // exclude single line annotation except expression keywords
                        if (!trimLine.startsWith("--") || StringUtil.startsWithsIgnoreCase(trimLine, IF, FI, CHOOSE, END)) {
                            if (!previousSqlName.equals("")) {
                                String prepareLine = singleResource.get(previousSqlName) + line;
                                if (trimLine.endsWith(";")) {
                                    String naSql = removeAnnotationBlock(prepareLine);
                                    singleResource.put(previousSqlName, naSql.substring(0, naSql.lastIndexOf(";")));
                                    log.debug("scan to get SQL [{}]：{}", previousSqlName, SqlUtil.highlightSql(singleResource.get(previousSqlName)));
                                    previousSqlName = "";
                                } else {
                                    singleResource.put(previousSqlName, prepareLine.concat("\n"));
                                }
                            }
                        }
                    }
                }
            }
            // if last part of sql is not ends with ';' symbol
            if (!previousSqlName.equals("")) {
                String lastSql = singleResource.get(previousSqlName);
                singleResource.put(previousSqlName, removeAnnotationBlock(lastSql));
                log.debug("scan to get SQL [{}]：{}", previousSqlName, SqlUtil.highlightSql(lastSql));
            }
        }
        mergeSqlPartIfNecessary(singleResource, prefix);
        RESOURCE.putAll(singleResource);
    }

    /**
     * 执行合并SQL片段
     *
     * @param partName    sql片段名
     * @param prefix      sql名前缀，自定义名或sql文件路径名
     * @param sqlResource sql字符串文件资源
     */
    private void doMergeSqlPart(final String partName, final String prefix, Map<String, String> sqlResource) {
        // inner sql part name like: '${filename.part1}'
        //innerPartName will be '${part1}'
        String innerPartName = "${" + partName.substring(prefix.length() + 2);
        for (String key : sqlResource.keySet()) {
            Pair<String, Map<String, String>> sqlAndSubstr = SqlUtil.replaceSqlSubstr(sqlResource.get(key));
            // get sql without substr first.
            String sql = sqlAndSubstr.getItem1();
            if (sql.contains(innerPartName)) {
                String part = sqlResource.get(partName);
                // insert sql part first without substr because we not allow substr sql part e.g. '${partName}'
                sql = sql.replace(innerPartName, part);
                // reinsert substr into the sql finally
                Map<String, String> substr = sqlAndSubstr.getItem2();
                if (!substr.isEmpty()) {
                    for (String substrKey : substr.keySet()) {
                        sql = sql.replace(substrKey, substr.get(substrKey));
                    }
                }
                sqlResource.put(key, sql);
            }
        }
    }

    /**
     * 合并SQL可复用片段到包含片段名的SQL中
     *
     * @param sqlResource sql字符串资源
     * @param prefix      sql名前缀，自定义名或sql文件路径名
     */
    private void mergeSqlPartIfNecessary(Map<String, String> sqlResource, String prefix) {
        for (String key : sqlResource.keySet()) {
            if (key.startsWith("${")) {
                doMergeSqlPart(key, prefix, sqlResource);
            }
        }
        if (constants != null && !constants.isEmpty()) {
            for (String name : sqlResource.keySet()) {
                Pair<String, Map<String, String>> sqlAndSubstr = SqlUtil.replaceSqlSubstr(sqlResource.get(name));
                // get sql without substr first.
                String sql = sqlAndSubstr.getItem1();
                for (String key : constants.keySet()) {
                    String constantName = "${" + key + "}";
                    if (sql.contains(constantName)) {
                        String constant = getConstant(key);
                        // insert sql part first without substr because we not allow substr sql part e.g. '${partName}'
                        sql = sql.replace(constantName, constant);
                    } else {
                        break;
                    }
                }
                // then reinsert substr into the sql
                Map<String, String> substr = sqlAndSubstr.getItem2();
                if (!substr.isEmpty()) {
                    for (String substrKey : substr.keySet()) {
                        sql = sql.replace(substrKey, substr.get(substrKey));
                    }
                }
                sqlResource.put(name, sql);
            }
        }
    }

    /**
     * 获取没有命名的防止重复的自增sql文件路径名
     *
     * @return sql路径名
     */
    private String getUnnamedPathName() {
        return UN_NAMED_SQL_NAME + UN_NAMED_SQL_INDEX.getAndIncrement();
    }

    /**
     * 整合所有命名和无命名Sql
     *
     * @return 所有sql
     */
    private Map<String, String> allPaths() {
        Map<String, String> pathMap = new LinkedHashMap<>();
        // add unnamed paths
        if (sqlList != null && sqlList.size() > 0) {
            for (String path : sqlList) {
                pathMap.put(getUnnamedPathName(), path);
            }
        }
        // add paths from constructor's args
        if (sqls != null && sqls.length > 0) {
            for (String path : sqls) {
                pathMap.put(getUnnamedPathName(), path);
            }
        }
        // add named paths
        if (sqlMap != null && sqlMap.size() > 0) {
            pathMap.putAll(sqlMap);
        }
        return pathMap;
    }

    /**
     * 如果有检测到文件修改过，则重新加载已修改过的sql文件
     *
     * @throws IOException           如果sql文件读取错误
     * @throws URISyntaxException    如果sql文件路径格式错误
     * @throws FileNotFoundException 如果sql文件不存在或路径无效
     */
    private void reloadIfNecessary() throws IOException, URISyntaxException {
        lock.lock();
        try {
            Map<String, String> mappedPaths = allPaths();
            for (String name : mappedPaths.keySet()) {
                FileResource cr = new FileResource(mappedPaths.get(name));
                if (cr.exists()) {
                    String suffix = cr.getFilenameExtension();
                    if (suffix != null && suffix.equals("sql")) {
                        String fileName = cr.getFileName();
                        if (LAST_MODIFIED.containsKey(fileName)) {
                            long timestamp = LAST_MODIFIED.get(fileName);
                            long lastModified = cr.getLastModified();
                            if (timestamp != -1 && timestamp != 0 && timestamp != lastModified) {
                                log.debug("removing expired SQL cache...");
                                resolveSqlContent(name, cr);
                                LAST_MODIFIED.put(fileName, lastModified);
                                log.debug("reload modified sql file:{}", fileName);
                            }
                        } else {
                            log.debug("load new sql file:{}", fileName);
                            resolveSqlContent(name, cr);
                            LAST_MODIFIED.put(fileName, cr.getLastModified());
                        }
                    }
                } else {
                    throw new FileNotFoundException("sql file of name'" + name + "' not found!");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 解析一条动态sql<br>
     * e.g. data.sql.template
     * <blockquote>
     * <pre>
     *         select *
     * from test.student t
     * WHERE
     * --#choose
     *     --#if :age{@code <} 21
     *     t.age = 21
     *     --#fi
     *     --#if :age{@code  <>} blank{@code &&} :age{@code <} 90
     *     and age{@code <} 90
     *     --#fi
     * --#end
     * --#if :name != null
     * and t.name ~ :name
     * --#fi
     * ;
     *     </pre>
     * </blockquote>
     *
     * @param sql          动态sql字符串
     * @param args         动态sql逻辑表达式参数字典
     * @param checkArgsKey 检查参数中是否必须存在表达式中需要计算的key
     * @return 解析后的sql
     * @throws IllegalArgumentException 如果 {@code checkArgsKey} 为 {@code true} 并且 {@code args} 中不存在表达式所需要的key
     * @throws NullPointerException     如果 {@code args} 为null
     * @see FastExpression
     */
    public static String calcDynamicSql(String sql, Map<String, ?> args, boolean checkArgsKey) {
        if (!containsAllIgnoreCase(sql, IF, FI)) {
            return sql;
        }
        String nSql = removeAnnotationBlock(sql);
        String[] lines = nSql.split("\n");
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        boolean ok = true;
        boolean start = false;
        boolean inBlock = false;
        boolean blockFirstOk = false;
        boolean hasChooseBlock = containsAllIgnoreCase(sql, CHOOSE, END);
        for (String line : lines) {
            String trimLine = line.trim();
            if (!trimLine.isEmpty()) {
                if (first) {
                    if (!trimLine.startsWith("--")) {
                        first = false;
                    }
                }
                if (hasChooseBlock) {
                    if (startsWithIgnoreCase(trimLine, CHOOSE)) {
                        blockFirstOk = false;
                        inBlock = true;
                        continue;
                    }
                    if (startsWithIgnoreCase(trimLine, END)) {
                        inBlock = false;
                        continue;
                    }
                }
                if (startsWithIgnoreCase(trimLine, IF) && !start) {
                    start = true;
                    if (inBlock) {
                        if (!blockFirstOk) {
                            String filter = trimLine.substring(5);
                            FastExpression expression = FastExpression.of(filter);
                            expression.setCheckArgsKey(checkArgsKey);
                            ok = expression.calc(args);
                            blockFirstOk = ok;
                        } else {
                            ok = false;
                        }
                    } else {
                        String filter = trimLine.substring(5);
                        FastExpression expression = FastExpression.of(filter);
                        expression.setCheckArgsKey(checkArgsKey);
                        ok = expression.calc(args);
                    }
                    continue;
                }
                if (startsWithIgnoreCase(trimLine, FI) && start) {
                    ok = true;
                    start = false;
                    continue;
                }
                if (ok) {
                    sb.append(line).append("\n");
                    if (!inBlock) {
                        blockFirstOk = false;
                    }
                }
            }
        }
        return repairSyntaxError(sb.toString());
    }

    /**
     * 修复sql常规语法错误<br>
     * e.g.
     * <blockquote>
     * <pre>where and/or/order/limit...</pre>
     * <pre>select ... from ...where</pre>
     * <pre>update ... set  a=b, where</pre>
     * </blockquote>
     *
     * @param sql sql语句
     * @return 修复后的sql
     */
    private static String repairSyntaxError(String sql) {
        Pattern p;
        Matcher m;
        String firstLine = sql.substring(0, sql.indexOf("\n")).trim();
        // if update statement
        if (startsWithIgnoreCase(firstLine, "update")) {
            p = Pattern.compile(",\\s*where", Pattern.CASE_INSENSITIVE);
            m = p.matcher(sql);
            if (m.find()) {
                sql = sql.substring(0, m.start()).concat(sql.substring(m.start() + 1));
            }
        }
        // "where and" statement
        p = Pattern.compile("where\\s+(and|or)\\s+", Pattern.CASE_INSENSITIVE);
        m = p.matcher(sql);
        if (m.find()) {
            return sql.substring(0, m.start() + 6).concat(sql.substring(m.end()));
        }
        // if "where order by ..." statement
        p = Pattern.compile("where\\s+(order by|limit|group by|union|\\))\\s+", Pattern.CASE_INSENSITIVE);
        m = p.matcher(sql);
        if (m.find()) {
            return sql.substring(0, m.start()).concat(sql.substring(m.start() + 6));
        }
        // if "where" at end
        p = Pattern.compile("where\\s*$", Pattern.CASE_INSENSITIVE);
        m = p.matcher(sql);
        if (m.find()) {
            return sql.substring(0, m.start());
        }
        return sql;
    }

    /**
     * 初始化加载sql到缓存中
     *
     * @throws IOException           如果sql文件读取错误
     * @throws URISyntaxException    如果sql文件路径格式错误
     * @throws FileNotFoundException 如果sql文件没有找到
     * @throws DuplicateException    如果同一个sql文件中有重复的sql名
     */
    public void init() throws IOException, URISyntaxException {
        Map<String, String> mappedPaths = allPaths();
        for (String name : mappedPaths.keySet()) {
            FileResource cr = new FileResource(mappedPaths.get(name));
            if (cr.exists()) {
                String suffix = cr.getFilenameExtension();
                if (suffix != null && suffix.equals("sql")) {
                    resolveSqlContent(name, cr);
                    LAST_MODIFIED.put(cr.getFileName(), cr.getLastModified());
                }
            } else {
                throw new FileNotFoundException("sql file of name'" + name + "' not found!");
            }
        }
    }

    /**
     * 是否已进行过初始化
     *
     * @return 初始化状态
     */
    public boolean isInitialized() {
        return !RESOURCE.isEmpty() && !LAST_MODIFIED.isEmpty();
    }

    /**
     * 遍历查看已扫描的sql资源
     */
    public void look() {
        RESOURCE.forEach((k, v) -> {
            Color color = Color.PURPLE;
            if (k.startsWith("${")) {
                color = Color.GREEN;
            }
            System.out.println(Printer.colorful(k, color) + " -> " + SqlUtil.highlightSql(v));
        });
    }

    /**
     * 遍历sql名和sql代码
     *
     * @param kvFunc 回调函数
     */
    public void foreach(BiConsumer<String, String> kvFunc) {
        RESOURCE.forEach(kvFunc);
    }

    /**
     * 获取全部sql名集合
     *
     * @return sql名集合
     */
    public Set<String> names() {
        return RESOURCE.keySet();
    }

    /**
     * 查看一共有多少条sql
     *
     * @return sql总条数
     */
    public int size() {
        return RESOURCE.size();
    }

    /**
     * 是否包含指定名称的sql
     *
     * @param name sql名<br>
     *             <blockquote>
     *             命名sql格式：sql文件命名.sql名<br>
     *             未命名sql格式：UN_NAMED_SQL_序号.sql名
     *             </blockquote>
     * @return 是否存在
     */
    public boolean containsSql(String name) {
        return RESOURCE.containsKey(name);
    }

    /**
     * 获取一条sql
     *
     * @param name sql名
     * @return sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws IORuntimeException     如果 {@code checkModified} 属性为true重载sql文件发生错误
     */
    public String get(String name) {
        if (checkModified) {
            try {
                reloadIfNecessary();
            } catch (URISyntaxException | IOException e) {
                throw new IORuntimeException("reload sql file error: ", e);
            }
        }
        if (RESOURCE.containsKey(name)) {
            return RESOURCE.get(name);
        }
        throw new NoSuchElementException(String.format("no SQL named [%s] was found.", name));
    }

    /**
     * 获取一条动态sql<br>
     * e.g. data.sql.template
     * <blockquote>
     * <pre>
     *         select *
     * from test.student t
     * WHERE
     * --#choose
     *     --#if :ageX{@code <} 21
     *     t.age = 21
     *     --#fi
     *     --#if :age{@code <>} blank{@code &&} :age{@code <} 90
     *     and age{@code <} 90
     *     --#fi
     * --#end
     * --#if :name != null
     * and t.name ~ :name
     * --#fi
     * ;
     *     </pre>
     * </blockquote>
     *
     * @param name         sql名字
     * @param args         动态sql逻辑表达式参数字典
     * @param checkArgsKey 检查参数中是否必须存在表达式中需要计算的key
     * @return 解析后的sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws IORuntimeException     如果 {@code checkModified} 属性为true重载sql文件发生错误
     * @see #calcDynamicSql(String, Map, boolean)
     */
    public String get(String name, Map<String, ?> args, boolean checkArgsKey) {
        String sql = get(name);
        return calcDynamicSql(sql, args, checkArgsKey);
    }

    /**
     * 获取一条动态sql<br>
     * e.g. data.sql.template
     * <blockquote>
     * <pre>
     *         select *
     * from test.student t
     * WHERE
     * --#choose
     *     --#if :age{@code <} 21
     *     t.age = 21
     *     --#fi
     *     --#if :age{@code <>} blank{@code &&} :age{@code <} 90
     *     and age{@code <} 90
     *     --#fi
     * --#end
     * --#if :name != null
     * and t.name ~ :name
     * --#fi
     * ;
     *     </pre>
     * </blockquote>
     *
     * @param name sql名字
     * @param args 动态sql逻辑表达式参数字典
     * @return 解析后的sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws IORuntimeException     如果 {@code checkModified} 属性为true重载sql文件发生错误
     * @see #calcDynamicSql(String, Map, boolean)
     */
    public String get(String name, Map<String, ?> args) {
        return get(name, args, true);
    }

    /**
     * 是否在每次获取sql时都检查文件是否更新
     *
     * @param checkModified 是否检查更新
     */
    public void setCheckModified(boolean checkModified) {
        this.checkModified = checkModified;
    }

    /**
     * 设置全局常量集合<br>
     * 初始化扫描sql时，如果sql文件中没有找到匹配的字符串模版，则从全局常量中寻找
     * 格式为：
     * <blockquote>
     * <pre>constants: {db:"test"}</pre>
     * <pre>sql: {@code select ${db}.user from table;}</pre>
     * <pre>result: select test.user from table.</pre>
     * </blockquote>
     *
     * @param constants 常量集合
     */
    public void setConstants(Map<String, String> constants) {
        this.constants = constants;
        log.debug("global constants defined: {}", constants);
    }

    /**
     * 获取常量集合
     *
     * @return 常量集合
     */
    public Map<String, String> getConstants() {
        return constants;
    }

    /**
     * 根据键获取常量值
     *
     * @param key 常量名
     * @return 常量值
     */
    public String getConstant(String key) {
        return constants.get(key);
    }
}
