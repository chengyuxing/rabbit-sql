package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.Comparators;
import com.github.chengyuxing.common.script.impl.FastExpression;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.exceptions.DynamicSQLException;
import com.github.chengyuxing.sql.exceptions.IORuntimeException;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.*;
import static com.github.chengyuxing.sql.utils.SqlUtil.removeAnnotationBlock;

/**
 * <h1>支持扩展脚本解析动态SQL的文件管理器</h1>
 * <h2>文件配置</h2>
 * <p>支持外部sql(本地文件系统)和classpath下的sql，
 * 本地sql文件以 {@code file:} 开头，默认读取<strong>classpath</strong>下的sql文件</p>
 * <p>识别的文件格式支持: {@code .xql.sql}</p>
 * e.g.
 * <blockquote>
 *     <ul>
 *         <li><pre>windows: file:\\D:\\rabbit.s(x)ql</pre></li>
 *         <li><pre>Linux/Unix: file:/root/rabbit.s(x)ql</pre></li>
 *         <li><pre>ClassPath: sql/rabbit.s(x)ql</pre></li>
 *     </ul>
 * </blockquote>
 * <h2>动态sql支持语法</h2>
 * <h3>if语句</h3>
 * <blockquote>
 * 支持嵌套if，choose，switch
 * <pre>
 * --#if 表达式1
 *      --#if 表达式2
 *      ...
 *      --#fi
 * --#fi
 * </pre>
 * </blockquote>
 * <h3>choose语句块</h3>
 * <blockquote>
 *  分支中还可以嵌套if语句
 * <pre>
 * --#choose
 *      --#when 表达式1
 *      ...
 *      --#break
 *      --#when 表达式2
 *      ...
 *      --#break
 *      ...
 *      --#default
 *      ...
 *      --#break
 * --#end
 * </pre>
 * </blockquote>
 * <h3>switch语句块</h3>
 * <blockquote>
 *  分支中还可以嵌套if语句
 * <pre>
 * --#switch :变量
 *      --#case 值1
 *      ...
 *      --#break
 *      --#case 值2
 *      ...
 *      --#break
 *      ...
 *      --#default
 *      ...
 *      --#break
 * --#end
 * </pre>
 * </blockquote>
 * <h3>e.g.</h3>
 * <blockquote>
 * <pre>
 * select *
 * from test.student t
 * WHERE
 * --#choose
 *      --#when :age{@code <} 21
 *      t.age = 21
 *      --#break
 *      --#when :age{@code <>} blank{@code &&} :age{@code <} 90
 *      and age{@code <} 90
 *      --#break
 *      --#default
 *      and age = 89
 *      --#break
 *  --#end
 *  --#if :name != null
 *      and t.name ~ :name
 *  --#fi
 * ;
 *     </pre>
 * </blockquote>
 * <h2>参考：</h2>
 * <blockquote>
 * data.xql.template
 * </blockquote>
 */
public class XQLFileManager {
    private final static Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, String> RESOURCE = new HashMap<>();
    private final Map<String, Long> LAST_MODIFIED = new HashMap<>();
    private static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    private static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");
    public static final String IF = "--#if";
    public static final String FI = "--#fi";
    public static final String CHOOSE = "--#choose";
    public static final String WHEN = "--#when";

    public static final String SWITCH = "--#switch";
    public static final String CASE = "--#case";

    public static final String DEFAULT = "--#default";
    public static final String BREAK = "--#break";
    public static final String END = "--#end";
    // ----------------optional properties------------------
    private boolean checkModified;
    private Map<String, String> constants = new HashMap<>();
    private Map<String, String> files = new HashMap<>();

    /**
     * Sql文件解析器实例
     */
    public XQLFileManager() {
    }

    /**
     * Sql文件解析器实例<br>
     *
     * @param files 文件：[别名，文件名]
     */
    public XQLFileManager(Map<String, String> files) {
        this.files = files;
    }

    /**
     * 添加命名的sql文件
     *
     * @param alias    文件别名
     * @param fileName 文件全路径名
     */
    public void add(String alias, String fileName) {
        files.put(alias, fileName);
    }

    /**
     * 设置命名的sql文件
     *
     * @param files 文件 [别名，文件名]
     */
    public void setFiles(Map<String, String> files) {
        if (!this.files.isEmpty()) {
            this.files.putAll(files);
        } else {
            this.files = files;
        }
    }

    /**
     * 解析sql文件
     *
     * @param name     文件别名
     * @param resource 文件资源
     * @throws IOException        如果文件不存在或路径无效
     * @throws DuplicateException 如果同一个文件中有重复的内容命名
     */
    protected void resolveSqlContent(String name, FileResource resource) throws IOException {
        Map<String, String> singleResource = new HashMap<>();
        String prefix = name + ".";
        String blockName = "";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (!trimLine.isEmpty()) {
                    Matcher m_name = NAME_PATTERN.matcher(trimLine);
                    Matcher m_part = PART_PATTERN.matcher(trimLine);
                    if (m_name.matches()) {
                        blockName = prefix + m_name.group("name");
                        if (singleResource.containsKey(blockName)) {
                            throw new DuplicateException("same block fragment name: " + blockName);
                        }
                        singleResource.put(blockName, "");
                    } else if (m_part.matches()) {
                        blockName = "${" + prefix + m_part.group("part") + "}";
                        if (singleResource.containsKey(blockName)) {
                            throw new DuplicateException("same block template name: " + blockName);
                        }
                        singleResource.put(blockName, "");
                    } else {
                        // exclude single line annotation except expression keywords
                        if (!trimLine.startsWith("--") || StringUtil.startsWithsIgnoreCase(trimLine, IF, FI, CHOOSE, WHEN, SWITCH, CASE, DEFAULT, BREAK, END)) {
                            if (!blockName.equals("")) {
                                String prepareLine = singleResource.get(blockName) + line;
                                if (trimLine.endsWith(";")) {
                                    String naSql = removeAnnotationBlock(prepareLine);
                                    singleResource.put(blockName, naSql.substring(0, naSql.lastIndexOf(";")));
                                    log.debug("scan to get block [{}]：{}", blockName, SqlUtil.highlightSql(singleResource.get(blockName)));
                                    blockName = "";
                                } else {
                                    singleResource.put(blockName, prepareLine.concat(NEW_LINE));
                                }
                            }
                        }
                    }
                }
            }
            // if last part of sql is not ends with ';' symbol
            if (!blockName.equals("")) {
                String lastSql = singleResource.get(blockName);
                singleResource.put(blockName, removeAnnotationBlock(lastSql));
                log.debug("scan to get block [{}]：{}", blockName, SqlUtil.highlightSql(lastSql));
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
    protected void doMergeSqlPart(final String partName, final String prefix, Map<String, String> sqlResource) {
        // inner sql part name like: '${filename.part1}'
        //innerPartName will be '${part1}'
        String innerPartName = "${" + partName.substring(prefix.length() + 2);
        for (String key : sqlResource.keySet()) {
            Pair<String, Map<String, String>> sqlAndSubstr = SqlUtil.replaceSqlSubstr(sqlResource.get(key));
            // get sql without substr first.
            String sql = sqlAndSubstr.getItem1();
            int partIndex;
            while ((partIndex = sql.indexOf(innerPartName)) != -1) {
                String part = NEW_LINE + sqlResource.get(partName).trim() + NEW_LINE;
                int start = StringUtil.searchIndexUntilNotBlank(sql, partIndex, true);
                int end = StringUtil.searchIndexUntilNotBlank(sql, partIndex + innerPartName.length() - 1, false);
                // insert sql part first without substr because we not allow substr sql part e.g. '${partName}'
                sql = sql.substring(0, start + 1) + part + sql.substring(end);
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
    protected void mergeSqlPartIfNecessary(Map<String, String> sqlResource, String prefix) {
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
     * 如果有检测到文件修改过，则重新加载已修改过的sql文件
     *
     * @throws IOException           如果sql文件读取错误
     * @throws URISyntaxException    如果sql文件路径格式错误
     * @throws FileNotFoundException 如果sql文件不存在或路径无效
     */
    protected void loadResource() throws IOException, URISyntaxException {
        lock.lock();
        try {
            for (String name : files.keySet()) {
                FileResource cr = new FileResource(files.get(name));
                if (cr.exists()) {
                    String suffix = cr.getFilenameExtension();
                    if (suffix != null && (suffix.equals("sql") || suffix.equals("xql"))) {
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
     * 初始化加载sql到缓存中
     *
     * @throws IOException           如果sql文件读取错误
     * @throws URISyntaxException    如果sql文件路径格式错误
     * @throws FileNotFoundException 如果sql文件没有找到
     * @throws DuplicateException    如果同一个sql文件中有重复的sql名
     */
    public void init() throws IOException, URISyntaxException {
        loadResource();
    }

    /**
     * 解析一条动态sql<br>
     *
     * @param sql          动态sql字符串
     * @param args         动态sql逻辑表达式参数字典
     * @param checkArgsKey 检查参数中是否必须存在表达式中需要计算的key
     * @return 解析后的sql
     * @throws IllegalArgumentException 如果 {@code checkArgsKey} 为 {@code true} 并且 {@code args} 中不存在表达式所需要的key
     * @throws NullPointerException     如果 {@code args} 为null
     * @see FastExpression
     */
    protected String dynamicCalc(String sql, Map<String, ?> args, boolean checkArgsKey) {
        String[] lines = sql.split(NEW_LINE);
        StringJoiner output = new StringJoiner(NEW_LINE);
        for (int i = 0, j = lines.length; i < j; i++) {
            String outerLine = lines[i];
            String trimOuterLine = outerLine.trim();
            int count = 0;
            // 处理if表达式块
            if (startsWithIgnoreCase(trimOuterLine, IF)) {
                count++;
                StringJoiner innerSb = new StringJoiner(NEW_LINE);
                // 内循环推进游标，用来判断嵌套if表达式块
                while (++i < j) {
                    String line = lines[i];
                    String trimLine = line.trim();
                    if (startsWithIgnoreCase(trimLine, IF)) {
                        innerSb.add(line);
                        count++;
                    } else if (startsWithIgnoreCase(trimLine, FI)) {
                        count--;
                        if (count < 0) {
                            throw new DynamicSQLException("can not find pair of 'if-fi' block at line " + i);
                        }
                        // 说明此处已经达到了嵌套fi的末尾
                        if (count == 0) {
                            // 此处计算外层if逻辑表达式，逻辑同程序语言的if逻辑
                            FastExpression fx = FastExpression.of(trimOuterLine.substring(5));
                            fx.setCheckArgsKey(checkArgsKey);
                            boolean res = fx.calc(args);
                            // 如果外层判断为真，如果内层还有if表达式块或choose...end块，则进入内层继续处理
                            // 否则就认为是原始sql逻辑判断需要保留片段
                            if (res) {
                                String innerStr = innerSb.toString();
                                if (containsAllIgnoreCase(innerStr, IF, FI) || containsAllIgnoreCase(innerStr, CHOOSE, END) || containsAllIgnoreCase(innerStr, SWITCH, END)) {
                                    output.add(dynamicCalc(innerStr, args, checkArgsKey));
                                } else {
                                    output.add(innerStr);
                                }
                            }
                            break;
                        } else {
                            // 说明此处没有达到外层fi，内层fi后面还有外层的sql表达式需要保留
                            // e.g.
                            // --#if
                            // ...
                            //      --#if
                            //      ...
                            //      --#fi
                            //      and t.a = :a    --此处为需要保留的地方
                            // --#fi
                            innerSb.add(line);
                        }
                    } else {
                        // 非表达式的部分sql需要保留
                        innerSb.add(line);
                    }
                }
                if (count != 0) {
                    throw new DynamicSQLException("can not find pair of 'if-fi' block at line " + i);
                }
                // 处理choose表达式块
            } else if (startsWithIgnoreCase(trimOuterLine, CHOOSE)) {
                while (++i < j) {
                    String line = lines[i];
                    String trimLine = line.trim();
                    if (startsWithsIgnoreCase(trimLine, WHEN, DEFAULT)) {
                        boolean res = false;
                        if (startsWithIgnoreCase(trimLine, WHEN)) {
                            FastExpression fx = FastExpression.of(trimLine.substring(7));
                            fx.setCheckArgsKey(checkArgsKey);
                            res = fx.calc(args);
                        }
                        // choose表达式块效果类似于程序语言的switch块，从前往后，只要满足一个分支，就跳出整个choose块
                        // 如果有default分支，前面所有when都不满足的情况下，就会直接选择default分支的sql作为结果保留
                        if (res || startsWithIgnoreCase(trimLine, DEFAULT)) {
                            StringJoiner innerSb = new StringJoiner(NEW_LINE);
                            // 移动游标直到此分支的break之前都是符合判断结果的sql保留下来
                            while (++i < j && !startsWithIgnoreCase(lines[i].trim(), BREAK)) {
                                if (startsWithsIgnoreCase(lines[i].trim(), WHEN, DEFAULT)) {
                                    throw new DynamicSQLException("missing '--#break' tag of expression '" + trimLine + "'");
                                }
                                innerSb.add(lines[i]);
                            }
                            String innerStr = innerSb.toString();
                            // when...break块中还可以包含if表达式块
                            if (containsAllIgnoreCase(innerStr, IF, FI)) {
                                output.add(dynamicCalc(innerStr, args, checkArgsKey));
                            } else {
                                output.add(innerStr);
                            }
                            // 到此处说明已经将满足条件的分支的sql保留下来
                            // 在接下来的分支都直接略过，移动游标直到end结束标签，就跳出整个choose块
                            //noinspection StatementWithEmptyBody
                            while (++i < j && !startsWithIgnoreCase(lines[i].trim(), END)) ;
                            if (i == j) {
                                throw new DynamicSQLException("missing '--#end' close tag of choose expression block.");
                            }
                            break;
                        } else {
                            // 如果此分支when语句表达式不满足条件，就移动游标到当前分支break结束，进入下一个when分支
                            while (++i < j && !startsWithIgnoreCase(lines[i].trim(), BREAK)) {
                                if (startsWithsIgnoreCase(lines[i].trim(), WHEN, DEFAULT)) {
                                    throw new DynamicSQLException("missing '--#break' tag of expression '" + trimLine + "'");
                                }
                            }
                        }
                    } else if (startsWithIgnoreCase(trimLine, END)) {
                        //在语句块为空的情况下，遇到end结尾，就跳出整个choose块
                        break;
                    } else {
                        output.add(line);
                    }
                }
                // 处理switch表达式块，逻辑等同于choose表达式块
            } else if (startsWithIgnoreCase(trimOuterLine, SWITCH)) {
                Pattern p = Pattern.compile(":(?<name>[\\S]+)");
                Matcher m = p.matcher(trimOuterLine.substring(9));
                String name = null;
                if (m.find()) {
                    name = m.group("name");
                }
                if (name == null) {
                    throw new DynamicSQLException("switch syntax error, cannot find var.");
                }
                Object value = args.get(name);
                while (++i < j) {
                    String line = lines[i];
                    String trimLine = line.trim();
                    if (startsWithsIgnoreCase(trimLine, CASE, DEFAULT)) {
                        boolean res = false;
                        if (startsWithIgnoreCase(trimLine, CASE)) {
                            res = Comparators.compare(value, "=", trimLine.substring(8));
                        }
                        if (res || startsWithIgnoreCase(trimLine, DEFAULT)) {
                            StringJoiner innerSb = new StringJoiner(NEW_LINE);
                            while (++i < j && !startsWithIgnoreCase(lines[i].trim(), BREAK)) {
                                if (startsWithsIgnoreCase(lines[i].trim(), CASE, DEFAULT)) {
                                    throw new DynamicSQLException("missing '--#break' tag of expression '" + trimLine + "'");
                                }
                                innerSb.add(lines[i]);
                            }
                            String innerStr = innerSb.toString();
                            // case...break块中还可以包含if表达式块
                            if (containsAllIgnoreCase(innerStr, IF, FI)) {
                                output.add(dynamicCalc(innerStr, args, checkArgsKey));
                            } else {
                                output.add(innerStr);
                            }
                            //noinspection StatementWithEmptyBody
                            while (++i < j && !startsWithIgnoreCase(lines[i].trim(), END)) ;
                            if (i == j) {
                                throw new DynamicSQLException("missing '--#end' close tag of switch expression block.");
                            }
                            break;
                        } else {
                            while (++i < j && !startsWithIgnoreCase(lines[i].trim(), BREAK)) {
                                if (startsWithsIgnoreCase(lines[i].trim(), WHEN, DEFAULT)) {
                                    throw new DynamicSQLException("missing '--#break' tag of expression '" + trimLine + "'");
                                }
                            }
                        }
                    } else if (startsWithIgnoreCase(trimLine, END)) {
                        break;
                    } else {
                        output.add(line);
                    }
                }
            } else {
                // 没有表达式的行，说明是原始sql的需要保留的部分
                output.add(outerLine);
            }
        }
        return output.toString();
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
    protected String repairSyntaxError(String sql) {
        Pattern p;
        Matcher m;
        // if update statement
        if (startsWithIgnoreCase(sql.trim(), "update")) {
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
        reloadIfNecessary();
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
        reloadIfNecessary();
        RESOURCE.forEach(kvFunc);
    }

    /**
     * 获取全部sql名集合
     *
     * @return sql名集合
     */
    public Set<String> names() {
        reloadIfNecessary();
        return RESOURCE.keySet();
    }

    /**
     * 查看一共有多少条sql
     *
     * @return sql总条数
     */
    public int size() {
        reloadIfNecessary();
        return RESOURCE.size();
    }

    /**
     * 是否包含指定名称的sql
     *
     * @param name sql名格式：别名.sql块命名<br>
     * @return 是否存在
     */
    public boolean contains(String name) {
        reloadIfNecessary();
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
        reloadIfNecessary();
        if (RESOURCE.containsKey(name)) {
            return RESOURCE.get(name);
        }
        throw new NoSuchElementException(String.format("no SQL named [%s] was found.", name));
    }

    /**
     * 获取一条动态sql<br>
     *
     * @param name         sql名字
     * @param args         动态sql逻辑表达式参数字典
     * @param checkArgsKey 检查参数中是否必须存在表达式中需要计算的key
     * @return 解析后的sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws IORuntimeException     如果 {@code checkModified} 属性为true重载sql文件发生错误
     * @see #dynamicCalc(String, Map, boolean)
     */
    public String get(String name, Map<String, ?> args, boolean checkArgsKey) {
        String sql = get(name);
        if (args == null || args.isEmpty()) {
            return sql;
        }
        return repairSyntaxError(dynamicCalc(sql, args, checkArgsKey));
    }

    /**
     * 获取一条动态sql<br>
     *
     * @param name sql名字
     * @param args 动态sql逻辑表达式参数字典
     * @return 解析后的sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws IORuntimeException     如果 {@code checkModified} 属性为true重载sql文件发生错误
     * @see #dynamicCalc(String, Map, boolean)
     */
    public String get(String name, Map<String, ?> args) {
        return get(name, args, true);
    }

    /**
     * 如果属性{@code checkModified}为 true 就进行文件检查修改更新
     */
    private void reloadIfNecessary() {
        if (checkModified) {
            try {
                loadResource();
            } catch (URISyntaxException | IOException e) {
                throw new IORuntimeException("reload sql file error: ", e);
            }
        }
    }

    /**
     * 设置在每次获取（{@code get(...)}）sql或调用其他查询方法（{@code look(), size()...}）时都检查文件是否更新
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
