package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.WatchDog;
import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;
import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.IExpression;
import com.github.chengyuxing.common.script.IPipe;
import com.github.chengyuxing.common.script.SimpleScriptParser;
import com.github.chengyuxing.common.script.impl.FastExpression;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.exceptions.DynamicSQLException;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.script.SimpleScriptParser.*;
import static com.github.chengyuxing.common.utils.StringUtil.NEW_LINE;
import static com.github.chengyuxing.sql.utils.SqlUtil.removeAnnotationBlock;

/**
 * <h2>支持扩展脚本解析动态SQL的文件管理器</h2>
 * 合理利用sql所支持的块注释（{@code /**}{@code /}）、行注释（{@code --}）、传名参数（{@code :name}）和字符串模版占位符（{@code ${template}}），对其进行了语法结构扩展，几乎没有对sql文件标准进行过改动，各种支持sql的IDE依然可以工作。<br>
 * 识别的文件格式支持: {@code .xql .sql}，两种文件内容没区别，{@code .xql} 结尾用来表示此类型文件是 {@code XQLFileManager} 所支持的扩展的sql文件。<br>
 * {@link FileResource 支持外部sql(URI)和classpath下的sql}，例如：
 * <ul>
 *     <li><pre>windows文件系统: file:/D:/rabbit.xql</pre></li>
 *     <li><pre>Linux/Unix文件系统: file:/root/rabbit.xql</pre></li>
 *     <li><pre>ClassPath: sql/rabbit.xql</pre></li>
 * </ul>
 * <h3>文件内容结构</h3>
 * <p>{@code key-value} 形式，key 为sql名，value为sql字符串，例如：</p>
 * <blockquote>
 *  /*[sq名1]*{@code /}<br>
 *  <pre>select * from test.region where
 *  -- #if :id != blank
 *      id = :id
 *  -- #fi
 * ${order};</pre>
 *  ...<br>
 *  /*[sql名n]*{@code /}<br>
 *    <pre>sql字符串n;</pre>
 *    ...<br>
 *  /*{order}*{@code /}<br>
 *    <pre>order by id desc;</pre>
 *    ...
 * </blockquote>
 * <p>{@link #get(String, Map)}, {@link #get(String, Map, boolean)}</p>
 * 动态sql脚本表达式写在行注释中 以 -- 开头，
 * 具体参考classpath下的文件：data.xql.template
 *
 * @see SimpleScriptParser
 */
public class XQLFileManager extends XQLFileManagerConfig implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    public static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    public static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");
    public static final String PROPERTIES = "xql-file-manager.properties";
    public static final String YML = "xql-file-manager.yml";
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Resource> resources = new HashMap<>();
    private final DynamicSqlParser dynamicSqlParser = new DynamicSqlParser();
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
     */
    public void remove(String alias) {
        lock.lock();
        try {
            resources.remove(alias);
            files.remove(alias);
            filenames.removeIf(filename -> StringUtil.getFileName(filename, false).equals(alias));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据文件名移除一个资源
     *
     * @param filename 文件名
     */
    public void removeByFilename(String filename) {
        lock.lock();
        try {
            filenames.remove(filename);
            files.entrySet().removeIf(next -> next.getValue().equals(filename));
            resources.entrySet().removeIf(e -> e.getValue().getFilename().equals(filename));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 清空所有已解析的sql资源
     */
    public void clearResources() {
        resources.clear();
    }

    /**
     * 清空所有文件以及解析的结果
     */
    public void clearFiles() {
        lock.lock();
        try {
            filenames.clear();
            files.clear();
            resources.clear();
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
            String defaultAlias = StringUtil.getFileName(fullName, false);
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
     * 更新sql文件资源
     *
     * @param alias    文件别名
     * @param filename 文件名
     * @param resource 文件资源
     * @throws IOException        如果文件不存在或路径无效
     * @throws DuplicateException 如果同一个文件中有重复的内容命名
     */
    protected void putResource(String alias, String filename, FileResource resource) throws IOException, URISyntaxException {
        resources.put(alias, parse(alias, filename, resource));
    }

    /**
     * 解析sql文件使之结构化
     *
     * @param filename     sql文件名
     * @param fileResource sql文件资源
     * @return 结构化的sql文件对象
     * @throws IOException        如果文件不存在或路径无效
     * @throws DuplicateException 如果同一个文件中有重复的内容命名
     * @throws URISyntaxException 如果文件URI格式错误
     */
    public Resource parse(String alias, String filename, FileResource fileResource) throws IOException, URISyntaxException {
        Resource resource = new Resource(alias, filename);
        Map<String, String> entry = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileResource.getInputStream(), charset))) {
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
                    if (entry.containsKey(blockName)) {
                        throw new DuplicateException("same sql fragment name: '" + blockName + "' in " + filename);
                    }
                    continue;
                }
                Matcher m_part = PART_PATTERN.matcher(trimLine);
                if (m_part.matches()) {
                    blockName = "${" + m_part.group("part") + "}";
                    if (entry.containsKey(blockName)) {
                        throw new DuplicateException("same sql template name: '" + blockName + "' in " + filename);
                    }
                    continue;
                }
                // exclude single line annotation except expression keywords
                if (!trimLine.startsWith("--") || (StringUtil.startsWithsIgnoreCase(dynamicSqlParser.trimExpression(trimLine), IF, FI, CHOOSE, WHEN, SWITCH, FOR, CASE, DEFAULT, BREAK, END))) {
                    if (!blockName.equals("")) {
                        sqlBodyBuffer.add(line);
                        if (trimLine.endsWith(delimiter)) {
                            String naSql = removeAnnotationBlock(String.join(NEW_LINE, sqlBodyBuffer));
                            entry.put(blockName, naSql.substring(0, naSql.lastIndexOf(delimiter)).trim());
                            log.debug("scan {} to get sql({}) [{}.{}]：{}", filename, delimiter, alias, blockName, SqlUtil.buildPrintSql(entry.get(blockName), highlightSql));
                            blockName = "";
                            sqlBodyBuffer.clear();
                        }
                    }
                }
            }
            // if last part of sql is not ends with delimiter symbol
            if (!blockName.equals("")) {
                String lastSql = String.join(NEW_LINE, sqlBodyBuffer);
                entry.put(blockName, removeAnnotationBlock(lastSql));
                log.debug("scan {} to get sql({}) [{}.{}]：{}", filename, delimiter, alias, blockName, SqlUtil.buildPrintSql(lastSql, highlightSql));
            }
        }
        if (!entry.isEmpty()) {
            mergeSqlTemplate(entry);
        }
        resource.setEntry(Collections.unmodifiableMap(entry));
        resource.setLastModified(fileResource.getLastModified());
        return resource;
    }

    /**
     * 合并SQL可复用片段到包含片段名的SQL中
     *
     * @param sqlResource sql字符串资源
     */
    protected void mergeSqlTemplate(Map<String, String> sqlResource) {
        for (String key : sqlResource.keySet()) {
            if (key.startsWith("${")) {
                for (Map.Entry<String, String> e : sqlResource.entrySet()) {
                    String sql = e.getValue();
                    String sqlPart = sqlResource.get(key);
                    String holder = key.substring(2, key.length() - 1);
                    sql = StringUtil.format(sql, holder, sqlPart);
                    e.setValue(sql);
                }
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
     * @throws UncheckedIOException 如果sql文件读取错误
     */
    protected void loadResource() {
        lock.lock();
        loading = true;
        try {
            Map<String, String> allFiles = allFiles();
            // 如果通过copyStateTo拷贝配置
            // 这里每次都会拷贝到配置文件中的files和filenames
            // 但解析的resources并不会同步更新
            // 可能resources中会存在着早已经删了文件的解析结果
            // 所以先进行删除没有对应的脏数据
            resources.entrySet().removeIf(e -> !allFiles.containsKey(e.getKey()));
            // 再完整的解析配置中的全部文件，确保文件和资源一一对应
            for (Map.Entry<String, String> fileE : allFiles.entrySet()) {
                String alias = fileE.getKey();
                String filename = fileE.getValue();
                FileResource cr = new FileResource(filename);
                if (cr.exists()) {
                    String suffix = cr.getFilenameExtension();
                    if (suffix != null && (suffix.equals("sql") || suffix.equals("xql"))) {
                        if (resources.containsKey(alias)) {
                            Resource resource = resources.get(alias);
                            long oldLastModified = resource.getLastModified();
                            long lastModified = cr.getLastModified();
                            if (oldLastModified != -1 && oldLastModified != 0 && oldLastModified != lastModified) {
                                putResource(alias, filename, cr);
                                log.debug("reload modified sql file: " + filename);
                            }
                        } else {
                            putResource(alias, filename, cr);
                        }
                    }
                } else {
                    throw new FileNotFoundException("sql file '" + filename + "' of name '" + alias + "' not found!");
                }
            }
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
                watchDog.addListener("sqlFileUpdateListener", this::loadResource, checkPeriod, TimeUnit.SECONDS);
            }
        } else {
            if (watchDog != null) {
                watchDog.removeListener("sqlFileUpdateListener");
                watchDog.shutdown();
            }
        }
    }

    /**
     * 遍历查看已扫描的sql资源
     */
    public void look() {
        foreach((k, v) -> v.getEntry().forEach((n, o) -> {
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
     * 遍历sql资源
     *
     * @param krFunc 回调函数 [alias, resource]
     */
    public void foreach(BiConsumer<String, Resource> krFunc) {
        resources.forEach(krFunc);
    }

    /**
     * 获取全部sql名集合
     *
     * @return sql名集合
     */
    public Set<String> names() {
        Set<String> names = new HashSet<>();
        foreach((k, r) -> r.getEntry().keySet().forEach(n -> names.add(k + "." + n)));
        return names;
    }

    /**
     * 查看一共有多少条sql
     *
     * @return sql总条数
     */
    public int size() {
        int i = 0;
        for (Resource resource : resources.values()) {
            i += resource.getEntry().size();
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
        String alias = name.substring(0, name.indexOf("."));
        if (!resources.containsKey(alias)) {
            return false;
        }
        String sqlName = name.substring(name.indexOf(".") + 1);
        return getResource(alias).getEntry().containsKey(sqlName);
    }

    /**
     * 获取sql解析资源
     *
     * @param alias sql文件别名
     * @return sql集
     */
    public Resource getResource(String alias) {
        return resources.get(alias);
    }

    /**
     * 是否存在资源
     *
     * @param alias 别名
     * @return 是否存在
     */
    public boolean containsResource(String alias) {
        return resources.containsKey(alias);
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
            Map<String, String> singleResource = getResource(alias).getEntry();
            String sqlName = name.substring(name.indexOf(".") + 1);
            if (singleResource.containsKey(sqlName)) {
                return SqlUtil.trimEnd(singleResource.get(sqlName));
            }
        }
        throw new NoSuchElementException(String.format("no SQL named [%s] was found.", name));
    }

    /**
     * 获取一条动态sql
     *
     * @param name         sql名字
     * @param args         动态sql逻辑表达式参数字典
     * @param checkArgsKey 检查参数中是否必须存在表达式中需要计算的key
     * @return 解析后的sql
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws DynamicSQLException    动态sql表达式解析异常
     * @see DynamicSqlParser
     */
    public String get(String name, Map<String, ?> args, boolean checkArgsKey) {
        String sql = get(name);
        try {
            sql = dynamicSqlParser.parse(sql, args, checkArgsKey);
            return SqlUtil.repairSyntaxError(sql);
        } catch (Exception e) {
            throw new DynamicSQLException("an error occurred when getting dynamic sql of name: " + name, e);
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
     * @see DynamicSqlParser
     */
    public String get(String name, Map<String, ?> args) {
        return get(name, args, true);
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
     * 获取是否已调用过初始化方法，此方法不影响重复初始化
     *
     * @return 是否已调用过初始化方法
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * 获取动态sql解析器
     *
     * @return 动态sql解析器
     */
    public DynamicSqlParser dynamicSqlParser() {
        return dynamicSqlParser;
    }

    /**
     * 关闭sql文件管理器释放所有文件资源
     */
    @Override
    public void close() {
        clearFiles();
        pipes.clear();
        pipeInstances.clear();
        constants.clear();
        if (watchDog != null) {
            watchDog.shutdown();
        }
    }

    /**
     * 动态sql解析器
     * {@inheritDoc}
     */
    public class DynamicSqlParser extends SimpleScriptParser {
        @Override
        protected IExpression expression(String expression) {
            FastExpression fastExpression = new FastExpression(expression);
            fastExpression.setPipes(getPipeInstances());
            return fastExpression;
        }

        @Override
        protected String forLoopBodyFormatter(String body, Map<String, Object> args) {
            return getSqlTranslator().formatSql(body, args);
        }

        @Override
        protected String trimExpression(String line) {
            String trimS = line.trim();
            if (trimS.startsWith("--")) {
                String expAnon = trimS.substring(2).trim();
                if (expAnon.startsWith("#")) {
                    return expAnon;
                }
            }
            return trimS;
        }
    }

    /**
     * sql文件资源
     */
    public static class Resource {
        private final String alias;
        private final String filename;
        private long lastModified = -1;
        private Map<String, String> entry;

        public Resource(String alias, String filename) {
            this.alias = alias;
            this.filename = filename;
            this.entry = Collections.emptyMap();
        }

        public String getAlias() {
            return alias;
        }

        public String getFilename() {
            return filename;
        }

        public long getLastModified() {
            return lastModified;
        }

        void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        public Map<String, String> getEntry() {
            return entry;
        }

        void setEntry(Map<String, String> entry) {
            if (entry == null) {
                return;
            }
            this.entry = entry;
        }
    }
}
