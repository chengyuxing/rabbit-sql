package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.IExpression;
import com.github.chengyuxing.common.script.IPipe;
import com.github.chengyuxing.common.script.SimpleScriptParser;
import com.github.chengyuxing.common.script.exception.ScriptSyntaxException;
import com.github.chengyuxing.common.script.impl.FastExpression;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.chengyuxing.common.script.SimpleScriptParser.TAGS;
import static com.github.chengyuxing.common.utils.StringUtil.*;
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
 * <p>{@link #get(String, Map)}</p>
 * 动态sql脚本表达式写在行注释中 以 -- 开头，
 * 具体参考classpath下的文件：template.xql
 *
 * @see SimpleScriptParser
 */
public class XQLFileManager extends XQLFileManagerConfig implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    //language=RegExp
    public static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    //language=RegExp
    public static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");
    public static final String PROPERTIES = "xql-file-manager.properties";
    public static final String YML = "xql-file-manager.yml";
    private final ClassLoader classLoader = this.getClass().getClassLoader();
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Resource> resources = new HashMap<>();
    private volatile boolean initialized;

    /**
     * XQL文件管理器<br>
     * 如果 <code>classpath</code> 下存在：
     * <ol>
     *     <li><code>xql-file-manager.yml</code></li>
     *     <li><code>xql-file-manager.properties</code></li>
     * </ol>
     * 则读取配置文件进行配置项初始化，如果文件同时存在，则读取yml。
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
     * XQL文件管理器
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
     * @return 文件别名
     */
    public String add(String fileName) {
        String alias = StringUtil.getFileName(fileName, false);
        add(alias, fileName);
        return alias;
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
            files.entrySet().removeIf(next -> next.getValue().equals(filename));
            resources.entrySet().removeIf(e -> e.getValue().getFilename().equals(filename));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 清空所有文件
     */
    public void clearFiles() {
        files.clear();
    }

    /**
     * 更新sql文件资源
     *
     * @param alias        文件别名
     * @param filename     文件名
     * @param fileResource 文件资源
     * @throws IOException        如果文件不存在或路径无效
     * @throws DuplicateException 如果同一个文件中有重复的内容命名
     * @throws URISyntaxException 如果文件URI格式错误
     */
    protected void putResource(String alias, String filename, FileResource fileResource) throws IOException, URISyntaxException {
        resources.put(alias, parse(alias, filename, fileResource));
    }

    /**
     * 解析sql文件使之结构化
     *
     * @param alias        文件别名
     * @param filename     sql文件名
     * @param fileResource sql文件资源
     * @return 结构化的sql文件对象
     * @throws IOException        如果文件不存在或路径无效
     * @throws DuplicateException 如果同一个文件中有重复的内容命名
     * @throws URISyntaxException 如果文件URI格式错误
     */
    public Resource parse(String alias, String filename, FileResource fileResource) throws IOException, URISyntaxException {
        Map<String, String> entry = new LinkedHashMap<>();
        try (BufferedReader reader = fileResource.getBufferedReader(Charset.forName(charset))) {
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
                if (!trimLine.startsWith("--") || (StringUtil.startsWithsIgnoreCase(trimAnnotation(trimLine), TAGS))) {
                    if (!blockName.isEmpty()) {
                        sqlBodyBuffer.add(line);
                        if (trimLine.endsWith(delimiter)) {
                            String naSql = removeAnnotationBlock(String.join(NEW_LINE, sqlBodyBuffer));
                            entry.put(blockName, naSql.substring(0, naSql.lastIndexOf(delimiter)).trim());
                            log.debug("scan {} to get sql({}) [{}.{}]：{}", filename, delimiter, alias, blockName, SqlUtil.buildConsoleSql(entry.get(blockName)));
                            blockName = "";
                            sqlBodyBuffer.clear();
                        }
                    }
                }
            }
            // if last part of sql is not ends with delimiter symbol
            if (!blockName.isEmpty()) {
                String lastSql = String.join(NEW_LINE, sqlBodyBuffer);
                entry.put(blockName, removeAnnotationBlock(lastSql));
                log.debug("scan {} to get sql({}) [{}.{}]：{}", filename, delimiter, alias, blockName, SqlUtil.buildConsoleSql(lastSql));
            }
        }
        if (!entry.isEmpty()) {
            mergeSqlTemplate(entry);
        }
        Resource resource = new Resource(alias, filename);
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
        Map<String, String> templates = new HashMap<>();
        for (Map.Entry<String, String> e : sqlResource.entrySet()) {
            String k = e.getKey();
            if (k.startsWith("${")) {
                templates.put(k.substring(2, k.length() - 1), e.getValue());
            }
        }
        if (templates.isEmpty() && constants.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> e : sqlResource.entrySet()) {
            String sql = e.getValue();
            if (sql.contains("${")) {
                sql = SqlUtil.formatSql(sql, templates);
                sql = SqlUtil.formatSql(sql, constants);
                e.setValue(sql);
            }
        }
    }

    /**
     * 如果有检测到文件修改过，则重新加载已修改过的sql文件
     *
     * @throws UncheckedIOException 如果sql文件读取错误
     * @throws RuntimeException     uri格式错误
     */
    protected void loadResources() {
        try {
            // 如果通过copyStateTo拷贝配置
            // 这里每次都会拷贝到配置文件中的files和filenames
            // 但解析的resources并不会同步更新
            // 可能resources中会存在着早已经删了文件的解析结果
            // 所以先进行删除没有对应的脏数据
            resources.entrySet().removeIf(e -> !files.containsKey(e.getKey()));
            // 再完整的解析配置中的全部文件，确保文件和资源一一对应
            for (Map.Entry<String, String> fileE : files.entrySet()) {
                String alias = fileE.getKey();
                String filename = fileE.getValue();
                FileResource cr = new FileResource(filename);
                if (cr.exists()) {
                    String ext = cr.getFilenameExtension();
                    if (ext != null && (ext.equals("sql") || ext.equals("xql"))) {
                        if (resources.containsKey(alias)) {
                            Resource resource = resources.get(alias);
                            long oldLastModified = resource.getLastModified();
                            long lastModified = cr.getLastModified();
                            if (oldLastModified > 0 && oldLastModified != lastModified) {
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
            throw new UncheckedIOException("load sql file error.", e);
        } catch (URISyntaxException e) {
            throw new RuntimeException("sql file uri syntax error.", e);
        }
    }

    /**
     * 加载管道
     */
    protected void loadPipes() {
        if (pipes.isEmpty()) return;
        try {
            for (Map.Entry<String, String> entry : pipes.entrySet()) {
                pipeInstances.put(entry.getKey(), (IPipe<?>) ReflectUtil.getInstance(classLoader.loadClass(entry.getValue())));
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                 InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("init pipe error.", e);
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
        lock.lock();
        try {
            loading = true;
            loadResources();
            loadPipes();
        } finally {
            loading = false;
            initialized = true;
            lock.unlock();
        }
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
        if (resources.containsKey(alias)) {
            String sqlName = name.substring(name.indexOf(".") + 1);
            return getResource(alias).getEntry().containsKey(sqlName);
        }
        return false;
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
     * 获取所有已解析的sql资源
     *
     * @return 不可修改的sql资源
     */
    public Map<String, Resource> getResources() {
        return Collections.unmodifiableMap(resources);
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
     * 清空所有已解析的sql资源
     */
    public void clearResources() {
        resources.clear();
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
     * @param name sql名字
     * @param args 动态sql逻辑表达式参数字典
     * @return 解析后的sql和#for表达式中的临时变量（如果有）
     * @throws NoSuchElementException 如果没有找到相应名字的sql片段
     * @throws ScriptSyntaxException  动态sql表达式解析异常
     * @see DynamicSqlParser
     */
    public Pair<String, Map<String, Object>> get(String name, Map<String, ?> args) {
        String sql = get(name);
        try {
            return parseDynamicSql(sql, args);
        } catch (Exception e) {
            throw new ScriptSyntaxException("an error occurred when getting dynamic sql of name: " + name, e);
        }
    }

    /**
     * 解析动态sql
     *
     * @param sql  动态sql
     * @param args 参数
     * @return 解析后的sql和#for表达式中的临时变量（如果有）
     */
    public Pair<String, Map<String, Object>> parseDynamicSql(String sql, Map<String, ?> args) {
        if (!containsAnyIgnoreCase(sql, TAGS)) {
            return Pair.of(sql, Collections.emptyMap());
        }
        DynamicSqlParser parser = newDynamicSqlParser();
        Map<String, Object> newArgs = new HashMap<>();
        if (Objects.nonNull(args)) {
            newArgs.putAll(args);
        }
        newArgs.put("_parameter", args);
        newArgs.put("_databaseId", databaseId);
        String parsedSql = parser.parse(sql, newArgs);
        parsedSql = SqlUtil.repairSyntaxError(parsedSql);
        return Pair.of(parsedSql, parser.getForVars());
    }

    /**
     * 根据键获取常量值
     *
     * @param key 常量名
     * @return 常量值
     */
    public Object getConstant(String key) {
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
     * 关闭sql文件管理器释放所有文件资源
     */
    @Override
    public void close() {
        lock.lock();
        try {
            clearFiles();
            clearResources();
            pipes.clear();
            pipeInstances.clear();
            constants.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 裁剪注释行以识别动态sql表达式前缀
     *
     * @param line 当前行
     * @return 注释行或表达式
     */
    protected String trimAnnotation(String line) {
        String trimS = line.trim();
        if (trimS.startsWith("--")) {
            String expAnon = trimS.substring(2).trim();
            if (expAnon.startsWith("#")) {
                return expAnon;
            }
        }
        return trimS;
    }

    /**
     * 获取一个新的动态sql解析器
     *
     * @return 动态sql解析器
     */
    public DynamicSqlParser newDynamicSqlParser() {
        return new DynamicSqlParser();
    }

    /**
     * 动态sql解析器
     * {@inheritDoc}
     */
    public class DynamicSqlParser extends SimpleScriptParser {
        public static final String FOR_VARS_KEY = "_for";
        public static final String VAR_PREFIX = FOR_VARS_KEY + ".";

        @Override
        protected IExpression expression(String expression) {
            FastExpression fastExpression = new FastExpression(expression);
            fastExpression.setPipes(getPipeInstances());
            return fastExpression;
        }

        /**
         * 去除for循环内的多余注释，为符合格式的命名参数进行编号
         *
         * @param forIndex 每个for循环语句的序号
         * @param varIndex for变量的序号
         * @param varName  for变量名
         * @param idxName  for变量序号名
         * @param body     for循环里的内容主体
         * @param args     用户参数和for循环每次迭代的参数（序号和值）
         * @return 经过格式化处理的内容
         */
        @Override
        protected String forLoopBodyFormatter(int forIndex, int varIndex, String varName, String idxName, List<String> body, Map<String, Object> args) {
            body.removeIf(l -> {
                String tl = l.trim();
                return tl.startsWith("--") && !tl.substring(2).trim().startsWith("#");
            });
            String formatted = SqlUtil.formatSql(String.join(NEW_LINE, body), args);
            if (varName != null) {
                String varParam = VAR_PREFIX + forVarKey(varName, forIndex, varIndex);
                formatted = formatted.replace(VAR_PREFIX + varName, varParam);
            }
            if (idxName != null) {
                String idxParam = VAR_PREFIX + forVarKey(idxName, forIndex, varIndex);
                formatted = formatted.replace(VAR_PREFIX + idxName, idxParam);
            }
            return formatted;
        }

        @Override
        protected String trimExpression(String line) {
            return trimAnnotation(line);
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
