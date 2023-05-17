package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.WatchDog;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.Comparators;
import com.github.chengyuxing.common.script.IExpression;
import com.github.chengyuxing.common.script.IPipe;
import com.github.chengyuxing.common.script.impl.FastExpression;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.exceptions.DynamicSQLException;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.utils.StringUtil.*;
import static com.github.chengyuxing.sql.utils.SqlUtil.removeAnnotationBlock;

/**
 * <h2>支持扩展脚本解析动态SQL的文件管理器</h2>
 * 合理利用sql所支持的块注释（{@code /**}{@code /}）、行注释（{@code --}）、传名参数（{@code :name}）和字符串模版占位符（{@code ${template}}），对其进行了语法结构扩展，几乎没有对sql文件标准进行过改动，各种支持sql的IDE依然可以工作。<br>
 * 识别的文件格式支持: {@code .xql .sql}，两种文件内容没区别，{@code .xql} 结尾用来表示此类型文件是 {@code XQLFileManager} 所支持的扩展的sql文件。<br>
 * {@link FileResource 支持外部sql(URI)和classpath下的sql}，例如：
 * <ul>
 *     <li><pre>windows文件系统: file:\\D:\\rabbit.xql</pre></li>
 *     <li><pre>Linux/Unix文件系统: file:/root/rabbit.xql</pre></li>
 *     <li><pre>ClassPath: sql/rabbit.xql</pre></li>
 * </ul>
 * <h3>文件内容结构</h3>
 * <p>{@code key-value} 形式，key 为sql名，value为sql字符串，例如：</p>
 * <blockquote>
 *  /*[sq名1]*{@code /}<br>
 *  <pre>select * from test.region where
 *  --#if :id != blank
 *      id = :id
 *  --#fi
 * ${order};</pre>
 *  ...<br>
 *  /*[sql名n]*{@code /}<br>
 *    <pre>sql字符串n;</pre>
 *    ...<br>
 *  /*{order}*{@code /}<br>
 *    <pre>order by id desc;</pre>
 *    ...
 * </blockquote>
 * <h3>动态sql</h3>
 * <p>{@link #get(String, Map)}, {@link #get(String, Map, boolean)}支持语法：</p>
 * <p>if语句块</p>
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
 * <p>choose语句块</p>
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
 * <p>switch语句块</p>
 * <blockquote>
 *  分支中还可以嵌套if语句
 * <pre>
 * --#switch :变量 | {@linkplain IPipe 管道1} | {@linkplain IPipe 管道n} | ...
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
 * <p>for语句块</p>
 * <blockquote>
 * 内部不能嵌套其他任何标签，不进行解析
 * <pre>
 * --#for item[,idx] of :list [| {@linkplain IPipe pipe1} | pipe2 | ... ] [delimiter ','] [filter{@code $}{item.name}[| {@linkplain IPipe pipe1} | pipe2 | ... ]{@code <>} blank]
 *     ...
 * --#end
 * </pre>
 * </blockquote>
 * 具体参考classpath下的文件：data.xql.template
 *
 * @see FastExpression
 */
public class XQLFileManager extends XQLFileManagerConfig implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Map<String, String>> resources = new HashMap<>();
    private final Map<String, Long> fileLastModifiedDates = new HashMap<>();
    private WatchDog watchDog = null;
    private volatile boolean initialized;

    /**
     * XQL文件管理器<br>
     * 如果 <code>classpath</code> 下存在：
     * <ol>
     *     <li><code>xql-file-manager.yml</code></li>
     *     <li><code>xql-file-manager.properties</code></li>
     * </ol>
     * 则读取配置文件进行配置项初始化，如果文件同时存在，则按顺序读取。
     *
     * @see XQLFileManagerConfig
     */
    public XQLFileManager() {
        FileResource resource = new FileResource(YML);
        if (resource.exists()) {
            loadYaml(resource);
            return;
        }
        resource = new FileResource(PROPERTIES);
        if (resource.exists()) {
            loadProperties(resource);
        }
    }

    /**
     * XQL文件管理器
     *
     * @param configLocation {@link FileResource 配置文件}：支持 {@code yml} 和 {@code properties}
     * @see XQLFileManagerConfig
     */
    public XQLFileManager(String configLocation) {
        super(configLocation);
    }

    /**
     * XQL文件管理器
     *
     * @param config 配置项
     */
    public XQLFileManager(XQLFileManagerConfig config) {
        config.copyStateTo(this);
    }

    /**
     * Sql文件解析器实例
     *
     * @param files 文件：[别名，文件名]
     */
    public XQLFileManager(Map<String, String> files) {
        if (files == null) {
            return;
        }
        this.files = new HashMap<>(files);
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
     * 添加sql文件，别名默认为文件名（不包含后缀）
     *
     * @param fileName 文件路径全名
     */
    public void add(String fileName) {
        this.filenames.add(fileName);
    }

    /**
     * 移除一个sql文件（sql资源也将被移除）
     *
     * @param alias 文件别名
     * @return 移除的sql文件名
     */
    public boolean remove(String alias) {
        lock.lock();
        try {
            boolean removed = false;
            resources.remove(alias);
            String fileFullName = files.remove(alias);
            if (fileFullName != null) {
                removed = true;
            }
            if (filenames.removeIf(fullName -> getFileNameWithoutExtension(fullName).equals(alias))) {
                removed = true;
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 统一集合所有命名sql和未命名sql文件
     *
     * @return 所有sql文件集合
     */
    public Map<String, String> allFiles() {
        Map<String, String> all = new HashMap<>(files);
        filenames.forEach(fullName -> {
            String defaultAlias = getFileNameWithoutExtension(fullName);
            if (all.containsKey(defaultAlias)) {
                throw new DuplicateException("auto generate sql alias error: '" + defaultAlias + "' already exists!");
            }
            if (all.containsValue(fullName)) {
                throw new DuplicateException("xql file '" + fullName + "' already configured, do not configure again!");
            }
            all.put(defaultAlias, fullName);
        });
        return all;
    }

    /**
     * 清空所有sql资源
     */
    public void clearSqlResources() {
        resources.clear();
    }

    /**
     * 清空所有文件
     */
    public void clearFiles() {
        lock.lock();
        try {
            filenames.clear();
            files.clear();
            fileLastModifiedDates.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取文件名不包含后缀
     *
     * @param fullFileName 文件全路径名
     * @return 文件名
     */
    protected String getFileNameWithoutExtension(String fullFileName) {
        String name;
        if (fullFileName.startsWith("file:")) {
            name = Paths.get(URI.create(fullFileName)).getFileName().toString();
        } else {
            name = Paths.get(fullFileName).getFileName().toString();
        }
        return name.substring(0, name.lastIndexOf("."));
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
        resources.put(name, sqlFileStructured(resource));
    }

    /**
     * 解析sql文件使之结构化
     *
     * @param resource sql文件资源
     * @return 结构化的sql文件对象
     * @throws IOException        如果文件不存在或路径无效
     * @throws DuplicateException 如果同一个文件中有重复的内容命名
     */
    public Map<String, String> sqlFileStructured(FileResource resource) throws IOException {
        Map<String, String> singleResource = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream(), charset))) {
            String blockName = "";
            List<String> sqlBodyBuffer = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (trimLine.isEmpty()) {
                    continue;
                }
                Matcher m_name = NAME_PATTERN.matcher(trimLine);
                if (m_name.matches()) {
                    blockName = m_name.group("name");
                    if (singleResource.containsKey(blockName)) {
                        throw new DuplicateException("same sql fragment name: '" + blockName + "' in " + resource.getPath());
                    }
                    continue;
                }
                Matcher m_part = PART_PATTERN.matcher(trimLine);
                if (m_part.matches()) {
                    blockName = "${" + m_part.group("part") + "}";
                    if (singleResource.containsKey(blockName)) {
                        throw new DuplicateException("same sql template name: '" + blockName + "' in " + resource.getPath());
                    }
                    continue;
                }
                // exclude single line annotation except expression keywords
                if (!trimLine.startsWith("--") || (StringUtil.startsWithsIgnoreCase(trimExp(trimLine), IF, FI, CHOOSE, WHEN, SWITCH, FOR, CASE, DEFAULT, BREAK, END))) {
                    if (!blockName.equals("")) {
                        sqlBodyBuffer.add(line);
                        if (trimLine.endsWith(delimiter)) {
                            String naSql = removeAnnotationBlock(String.join(NEW_LINE, sqlBodyBuffer));
                            singleResource.put(blockName, naSql.substring(0, naSql.lastIndexOf(delimiter)).trim());
                            log.debug("scan {} to get sql({}) [{}]：{}", resource.getFileName(), delimiter, blockName, SqlUtil.buildPrintSql(singleResource.get(blockName), highlightSql));
                            blockName = "";
                            sqlBodyBuffer.clear();
                        }
                    }
                }
            }
            // if last part of sql is not ends with delimiter symbol
            if (!blockName.equals("")) {
                String lastSql = String.join(NEW_LINE, sqlBodyBuffer);
                singleResource.put(blockName, removeAnnotationBlock(lastSql));
                log.debug("scan {} to get sql({}) [{}]：{}", resource.getFileName(), delimiter, blockName, SqlUtil.buildPrintSql(lastSql, highlightSql));
            }
        }
        if (!singleResource.isEmpty()) {
            mergeSqlPartIfNecessary(singleResource);
        }
        return singleResource;
    }

    /**
     * 执行合并SQL片段
     *
     * @param partName    sql片段名
     * @param sqlResource sql字符串文件资源
     */
    protected void doMergeSqlPart(final String partName, Map<String, String> sqlResource) {
        for (Map.Entry<String, String> e : sqlResource.entrySet()) {
            String sql = e.getValue();
            String sqlPart = sqlResource.get(partName);
            String key = partName.substring(2, partName.length() - 1);
            sql = StringUtil.format(sql, key, sqlPart);
            e.setValue(sql);
        }
    }

    /**
     * 合并SQL可复用片段到包含片段名的SQL中
     *
     * @param sqlResource sql字符串资源
     */
    protected void mergeSqlPartIfNecessary(Map<String, String> sqlResource) {
        for (String key : sqlResource.keySet()) {
            if (key.startsWith("${")) {
                doMergeSqlPart(key, sqlResource);
            }
        }
        if (constants != null && !constants.isEmpty()) {
            for (Map.Entry<String, String> sqlE : sqlResource.entrySet()) {
                String sql = sqlE.getValue();
                for (Map.Entry<String, String> constE : constants.entrySet()) {
                    sql = StringUtil.format(sql, constE.getKey(), constE.getValue());
                }
                sqlE.setValue(sql);
            }
        }
    }

    /**
     * 如果有检测到文件修改过，则重新加载已修改过的sql文件
     *
     * @return 已修改或新增的解析文件信息
     * @throws UncheckedIOException 如果sql文件读取错误
     */
    protected List<String> loadResource() {
        lock.lock();
        try {
            loading = true;
            List<String> msg = new ArrayList<>();
            for (Map.Entry<String, String> fileE : allFiles().entrySet()) {
                FileResource cr = new FileResource(fileE.getValue());
                if (cr.exists()) {
                    String suffix = cr.getFilenameExtension();
                    if (suffix != null && (suffix.equals("sql") || suffix.equals("xql"))) {
                        String fileName = cr.getFileName();
                        if (fileLastModifiedDates.containsKey(fileName)) {
                            long timestamp = fileLastModifiedDates.get(fileName);
                            long lastModified = cr.getLastModified();
                            if (timestamp != -1 && timestamp != 0 && timestamp != lastModified) {
                                resolveSqlContent(fileE.getKey(), cr);
                                fileLastModifiedDates.put(fileName, lastModified);
                                msg.add("reload modified sql file: " + fileName);
                            } else {
                                msg.add("sql file: " + fileName + " already loaded.");
                            }
                        } else {
                            resolveSqlContent(fileE.getKey(), cr);
                            fileLastModifiedDates.put(fileName, cr.getLastModified());
                            msg.add("load new sql file: " + fileName);
                        }
                    }
                } else {
                    throw new FileNotFoundException("sql file '" + fileE.getValue() + "' of name '" + fileE.getKey() + "' not found!");
                }
            }
            return msg;
        } catch (IOException e) {
            throw new UncheckedIOException("load sql file error: ", e);
        } catch (URISyntaxException e) {
            throw new RuntimeException("sql file uri syntax error: ", e);
        } finally {
            loading = false;
            lock.unlock();
        }
    }

    /**
     * 加载管道
     */
    protected void loadPipes() {
        if (!pipes.isEmpty()) {
            try {
                for (Map.Entry<String, String> entry : pipes.entrySet()) {
                    pipeInstances.put(entry.getKey(), (IPipe<?>) ReflectUtil.getInstance(Class.forName(entry.getValue())));
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                     InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException("init pipe error: ", e);
            }
        }
        if (log.isDebugEnabled()) {
            if (!pipeInstances.isEmpty())
                log.debug("loaded pipes {}", pipeInstances);
        }
    }

    /**
     * 初始化加载sql到缓存中
     *
     * @throws UncheckedIOException 如果sql文件读取错误或sql文件没有找到
     * @throws RuntimeException     如果sql文件路径格式错误
     * @throws DuplicateException   如果同一个sql文件中有重复的sql名
     */
    public void init() {
        initialized = true;
        loadResource();
        loadPipes();
        if (this.checkModified) {
            if (watchDog == null) {
                watchDog = new WatchDog(1);
                watchDog.addListener("sqlFileUpdateListener", () -> {
                    List<String> msg = loadResource();
                    if (log.isDebugEnabled()) {
                        if (msg.size() > 0) {
                            for (String m : msg) {
                                log.debug("sqlFileUpdateListener: {}", m);
                            }
                        }
                    }
                }, checkPeriod, TimeUnit.SECONDS);
            }
        } else {
            if (watchDog != null) {
                watchDog.removeListener("sqlFileUpdateListener");
                watchDog.shutdown();
            }
        }
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
    public String dynamicCalc(String sql, Map<String, ?> args, boolean checkArgsKey) {
        String[] lines = sql.split(NEW_LINE);
        StringJoiner output = new StringJoiner(NEW_LINE);
        for (int i = 0, j = lines.length; i < j; i++) {
            String outerLine = lines[i];
            String trimOuterLine = trimExp(outerLine);
            int count = 0;
            // 处理if表达式块
            if (startsWithIgnoreCase(trimOuterLine, IF)) {
                count++;
                StringJoiner innerSb = new StringJoiner(NEW_LINE);
                // 内循环推进游标，用来判断嵌套if表达式块
                while (++i < j) {
                    String line = lines[i];
                    String trimLine = trimExp(line);
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
                            IExpression fx = FastExpression.of(trimOuterLine.substring(3));
                            fx.setPipes(pipeInstances);
                            boolean res = fx.calc(args, checkArgsKey);
                            // 如果外层判断为真，如果内层还有if表达式块或choose...end块，则进入内层继续处理
                            // 否则就认为是原始sql逻辑判断需要保留片段
                            if (res) {
                                String innerStr = innerSb.toString();
                                if (containsAllIgnoreCase(innerStr, IF, FI) || containsAllIgnoreCase(innerStr, CHOOSE, END) || containsAllIgnoreCase(innerStr, SWITCH, END) || containsAllIgnoreCase(innerStr, FOR, END)) {
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
                    String trimLine = trimExp(line);
                    if (startsWithsIgnoreCase(trimLine, WHEN, DEFAULT)) {
                        boolean res = false;
                        if (startsWithIgnoreCase(trimLine, WHEN)) {
                            IExpression fx = FastExpression.of(trimLine.substring(5));
                            fx.setPipes(pipeInstances);
                            res = fx.calc(args, checkArgsKey);
                        }
                        // choose表达式块效果类似于程序语言的switch块，从前往后，只要满足一个分支，就跳出整个choose块
                        // 如果有default分支，前面所有when都不满足的情况下，就会直接选择default分支的sql作为结果保留
                        if (res || startsWithIgnoreCase(trimLine, DEFAULT)) {
                            StringJoiner innerSb = new StringJoiner(NEW_LINE);
                            // 移动游标直到此分支的break之前都是符合判断结果的sql保留下来
                            while (++i < j && !startsWithIgnoreCase(trimExp(lines[i]), BREAK)) {
                                if (startsWithsIgnoreCase(trimExp(lines[i]), WHEN, DEFAULT)) {
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
                            while (++i < j && !startsWithIgnoreCase(trimExp(lines[i]), END)) ;
                            if (i == j) {
                                throw new DynamicSQLException("missing '--#end' close tag of choose expression block.");
                            }
                            break;
                        } else {
                            // 如果此分支when语句表达式不满足条件，就移动游标到当前分支break结束，进入下一个when分支
                            while (++i < j && !startsWithIgnoreCase(trimExp(lines[i]), BREAK)) {
                                if (startsWithsIgnoreCase(trimExp(lines[i]), WHEN, DEFAULT)) {
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
                Matcher m = SWITCH_PATTERN.matcher(trimOuterLine.substring(7));
                String name = null;
                String pipes = null;
                if (m.find()) {
                    name = m.group("name");
                    pipes = m.group("pipes");
                }
                if (name == null) {
                    throw new DynamicSQLException("switch syntax error of expression '" + trimOuterLine + "', cannot find var.");
                }
                Object value = ObjectUtil.getValueWild(args, name);
                if (pipes != null && !pipes.trim().equals("")) {
                    value = FastExpression.of("empty").pipedValue(value, pipes);
                }
                while (++i < j) {
                    String line = lines[i];
                    String trimLine = trimExp(line);
                    if (startsWithsIgnoreCase(trimLine, CASE, DEFAULT)) {
                        boolean res = false;
                        if (startsWithIgnoreCase(trimLine, CASE)) {
                            res = Comparators.compare(value, "=", Comparators.valueOf(trimLine.substring(5).trim()));
                        }
                        if (res || startsWithIgnoreCase(trimLine, DEFAULT)) {
                            StringJoiner innerSb = new StringJoiner(NEW_LINE);
                            while (++i < j && !startsWithIgnoreCase(trimExp(lines[i]), BREAK)) {
                                if (startsWithsIgnoreCase(trimExp(lines[i]), CASE, DEFAULT)) {
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
                            while (++i < j && !startsWithIgnoreCase(trimExp(lines[i]), END)) ;
                            if (i == j) {
                                throw new DynamicSQLException("missing '--#end' close tag of switch expression block.");
                            }
                            break;
                        } else {
                            while (++i < j && !startsWithIgnoreCase(trimExp(lines[i]), BREAK)) {
                                if (startsWithsIgnoreCase(trimExp(lines[i]), WHEN, DEFAULT)) {
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
            } else if (startsWithIgnoreCase(trimOuterLine, FOR)) {
                Matcher m = FOR_PATTERN.matcher(trimOuterLine.substring(4).trim());
                if (m.find()) {
                    // 完整的表达式例如：item[,idx] of :list [| pipe1 | pipe2 | ... ] [delimiter ','] [filter ${item.name}[| pipe1 | pipe2 | ... ] <> blank]
                    // 方括号中为可选参数
                    String itemName = m.group("item");
                    String idxName = m.group("index");
                    String listName = m.group("list");
                    String pipes = m.group("pipes");
                    String delimiter = m.group("delimiter");
                    String filter = m.group("filter");
                    // 认为for表达式块中有多行需要迭代的sql片段，在此全部找出来用换行分割，保留格式
                    StringJoiner loopPart = new StringJoiner("\n");
                    while (++i < j && !startsWithIgnoreCase(trimExp(lines[i]), END)) {
                        loopPart.add(lines[i]);
                    }
                    Object loopObj = ObjectUtil.getValueWild(args, listName);
                    if (pipes != null && !pipes.trim().equals("")) {
                        loopObj = FastExpression.of("empty").pipedValue(loopObj, pipes);
                    }
                    Object[] loopArr;
                    if (loopObj instanceof Object[]) {
                        loopArr = (Object[]) loopObj;
                    } else if (loopObj instanceof Collection) {
                        //noinspection unchecked
                        loopArr = ((Collection<Object>) loopObj).toArray();
                    } else {
                        loopArr = new Object[]{loopObj};
                    }
                    // 如果没指定分割符，默认迭代sql片段最终使用逗号连接
                    StringJoiner forSql = new StringJoiner(delimiter == null ? ", " : delimiter.replace("\\n", "\n").replace("\\t", "\t"));
                    // 用于查找for定义变量的正则表达式
                    /// 需要验证下正则表达式 例如：user.address.street，超过2级
                    Pattern filterP = Pattern.compile("\\$\\{\\s*(?<tmp>(" + itemName + ")(.\\w+)*|" + idxName + ")\\s*}");
                    for (int x = 0; x < loopArr.length; x++) {
                        Map<String, Object> filterArgs = new HashMap<>();
                        filterArgs.put(itemName, loopArr[x]);
                        filterArgs.put(idxName, x);
                        // 如果定义了过滤器，首先对数据进行筛选操作，不满足条件的直接过滤
                        if (filter != null) {
                            // 查找过滤器中的引用变量
                            Matcher vx = filterP.matcher(filter);
                            Map<String, Object> filterTemps = new HashMap<>();
                            String expStr = filter;
                            while (vx.find()) {
                                String tmp = vx.group("tmp");
                                filterTemps.put(tmp, ":" + tmp);
                            }
                            // 将filter子句转为支持表达式解析的子句格式
                            expStr = StringUtil.format(expStr, filterTemps);
                            IExpression fx = FastExpression.of(expStr);
                            fx.setPipes(pipeInstances);
                            if (!fx.calc(filterArgs, checkArgsKey)) {
                                continue;
                            }
                        }
                        // 准备循环迭代生产满足条件的sql片段
                        String sqlPart = getSqlTranslator().formatSql(loopPart.toString().trim(), filterArgs);
                        forSql.add(sqlPart);
                    }
                    output.add(forSql.toString());
                } else {
                    throw new DynamicSQLException("for syntax error of expression '" + trimOuterLine + "' ");
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
     * 如果有必要则格式化注释表达式
     *
     * @param s 注释行
     * @return 前缀满足动态sql表达式的字符串或者首尾去除空白字符的字符串
     */
    protected String trimExp(String s) {
        String trimS = s.trim();
        if (trimS.startsWith("--")) {
            String expAnon = trimS.substring(2).trim();
            if (expAnon.startsWith("#")) {
                return expAnon;
            }
        }
        return trimS;
    }

    /**
     * 遍历查看已扫描的sql资源
     */
    public void look() {
        resources.forEach((k, v) -> v.forEach((n, o) -> {
            Color color = Color.PURPLE;
            if (n.startsWith("${")) {
                color = Color.GREEN;
            }
            String prefix = k + "." + n;
            if (highlightSql) {
                prefix = Printer.colorful(prefix, color);
            }
            System.out.println(prefix + " -> " + SqlUtil.buildPrintSql(o, highlightSql));
        }));
    }

    /**
     * 遍历sql名和sql代码
     *
     * @param kvFunc 回调函数
     */
    public void foreach(BiConsumer<String, String> kvFunc) {
        foreachSqlEntry((k, r) -> r.forEach((n, o) -> kvFunc.accept(k + "." + n, o)));
    }

    /**
     * 遍历sql名和sql集
     *
     * @param krFunc 回调函数
     */
    public void foreachSqlEntry(BiConsumer<String, Map<String, String>> krFunc) {
        resources.forEach(krFunc);
    }

    /**
     * 获取全部sql名集合
     *
     * @return sql名集合
     */
    public Set<String> names() {
        Set<String> names = new HashSet<>();
        foreachSqlEntry((k, r) -> r.keySet().forEach(n -> names.add(k + "." + n)));
        return names;
    }

    /**
     * 查看一共有多少条sql
     *
     * @return sql总条数
     */
    public int size() {
        int i = 0;
        for (Map<String, String> e : resources.values()) {
            i += e.size();
        }
        return i;
    }

    /**
     * 是否包含指定名称的sql
     *
     * @param name sql名格式：别名.sql块命名<br>
     * @return 是否存在
     */
    public boolean contains(String name) {
        String fileAlias = name.substring(0, name.indexOf("."));
        if (!resources.containsKey(fileAlias)) {
            return false;
        }
        String sqlName = name.substring(name.indexOf(".") + 1);
        return resources.get(fileAlias).containsKey(sqlName);
    }

    /**
     * 获取sql集
     *
     * @param name sql文件别名
     * @return sql集
     */
    public Map<String, String> getSqlEntry(String name) {
        return resources.get(name);
    }

    /**
     * 获取一条sql
     *
     * @param name sql名
     * @return sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     */
    public String get(String name) {
        String alias = name.substring(0, name.indexOf("."));
        if (resources.containsKey(alias)) {
            Map<String, String> singleResource = resources.get(alias);
            String sqlName = name.substring(name.indexOf(".") + 1);
            if (singleResource.containsKey(sqlName)) {
                return SqlUtil.trimEnd(singleResource.get(sqlName));
            }
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
     * @throws DynamicSQLException    动态sql表达式解析异常
     * @see #dynamicCalc(String, Map, boolean)
     */
    public String get(String name, Map<String, ?> args, boolean checkArgsKey) {
        String sql = get(name);
        try {
            return repairSyntaxError(dynamicCalc(sql, args, checkArgsKey));
        } catch (Exception e) {
            throw new DynamicSQLException("an error occurred when getting dynamic sql: " + sql, e);
        }
    }

    /**
     * 获取一条动态sql<br>
     *
     * @param name sql名字
     * @param args 动态sql逻辑表达式参数字典
     * @return 解析后的sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws DynamicSQLException    动态sql表达式解析异常
     * @see #dynamicCalc(String, Map, boolean)
     */
    public String get(String name, Map<String, ?> args) {
        return get(name, args, true);
    }

    /**
     * 获取是否已调用过初始化方法，此方法不影响重复初始化
     *
     * @return 是否已调用过初始化方法
     */
    public boolean isInitialized() {
        return initialized;
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

    /**
     * 关闭sql文件管理器释放所有文件资源
     */
    @Override
    public void close() {
        clearSqlResources();
        clearFiles();
        pipes.clear();
        pipeInstances.clear();
        constants.clear();
        if (watchDog != null) {
            watchDog.shutdown();
        }
    }
}
