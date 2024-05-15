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
import com.github.chengyuxing.sql.utils.SqlHighlighter;
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
import static com.github.chengyuxing.common.utils.StringUtil.NEW_LINE;
import static com.github.chengyuxing.common.utils.StringUtil.containsAnyIgnoreCase;
import static com.github.chengyuxing.sql.utils.SqlUtil.removeBlockAnnotation;

/**
 * <h2>Dynamic SQL parse file manager</h2>
 * <p>Use standard sql block annotation ({@code /**}{@code /}), line annotation ({@code --}),
 * named parameter ({@code :name}) and string template variable ({@code ${template}}) to
 * extends SQL file standard syntax with special content format, brings more features to
 * SQL file and follow the strict SQL file syntax.</p>
 * <p>File type supports: {@code .xql}, {@code .sql}, suffix {@code .xql} means this file is
 * {@code XQLFileManager} default file type.</p>
 * {@link FileResource Support local sql file(URI) and classpath sql file},  e.g.
 * <ul>
 *     <li>Windows file system: <code>file:/D:/rabbit.xql</code></li>
 *     <li>Linux/Unix file system: <code>file:/root/rabbit.xql</code></li>
 *     <li>ClassPath: <code>sql/rabbit.xql</code></li>
 * </ul>
 * <p>
 * Notice: <a href="https://plugins.jetbrains.com/plugin/21403-rabbit-sql/introduction/execute-dynamic-sql">Rabbit-SQL IDEA Plugin</a> only support detect {@code .xql} file.
 * <h3>File content structure</h3>
 * <p>{@code key-value} format, key is sql name, value is sql statement,  e.g.</p>
 * <blockquote>
 * <pre>
 * /&#42;[sqlName1]&#42;/
 * select * from test.region where
 *  -- #if :id != blank
 *     id = :id
 *  -- #fi
 * ${order}
 * ;
 *
 * /&#42;[sqlNameN]&#42;/
 * select * from test.user;
 *
 * /&#42;{order}&#42;/
 * order by id desc;
 * ...
 * </pre>
 * </blockquote>
 * <p>
 * Dynamic sql script write in line annotation where starts with {@code --},
 * check example following class path file: {@code template.xql}.
 * <p>Invoke method {@link #get(String, Map)} to enjoy the dynamic sql!</p>
 *
 * @see SimpleScriptParser
 */
public class XQLFileManager extends XQLFileManagerConfig implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    //language=RegExp
    public static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    //language=RegExp
    public static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");
    public static final String ANNO_START = "/*";
    public static final String ANNO_END = "*/";
    public static final String SQL_DESC_START = "/*#";
    public static final String SQL_DESC_END = "#*/";
    public static final String XQL_DESC_QUOTE = "###";
    public static final String PROPERTIES = "xql-file-manager.properties";
    public static final String YML = "xql-file-manager.yml";
    private final ClassLoader classLoader = this.getClass().getClassLoader();
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Resource> resources = new HashMap<>();
    private volatile boolean initialized;

    /**
     * Constructs a new XQLFileManager.<br>
     * If <code>classpath</code> exists files：
     * <ol>
     *     <li><code>xql-file-manager.yml</code></li>
     *     <li><code>xql-file-manager.properties</code></li>
     * </ol>
     * load {@code .yml} first otherwise {@code .properties}.
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
     * Constructs a new XQLFileManager with config location.
     *
     * @param configLocation {@link FileResource config file}: supports {@code .yml} and {@code .properties}
     * @see XQLFileManagerConfig
     */
    public XQLFileManager(String configLocation) {
        super(configLocation);
    }

    /**
     * Constructs a new XQLFileManager with xql file manager config.
     *
     * @param config xql file manager config
     */
    public XQLFileManager(XQLFileManagerConfig config) {
        config.copyStateTo(this);
    }

    /**
     * Constructs a new XQLFileManager with initial sql file map.
     *
     * @param files sql file: [alias, file path name]
     */
    public XQLFileManager(Map<String, String> files) {
        if (files == null) {
            return;
        }
        this.files = new HashMap<>(files);
    }

    /**
     * Add a sql file.
     *
     * @param alias    file alias
     * @param fileName file path name
     */
    public void add(String alias, String fileName) {
        files.put(alias, fileName);
    }

    /**
     * Add a sql file with default alias(file name without extension).
     *
     * @param fileName file path name
     * @return file alias
     */
    public String add(String fileName) {
        String alias = FileResource.getFileName(fileName, false);
        add(alias, fileName);
        return alias;
    }

    /**
     * Remove a sql file with associated sql resource.
     *
     * @param alias file alias
     */
    public void remove(String alias) {
        lock.lock();
        try {
            files.remove(alias);
            resources.remove(alias);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Put sql resource persistent.
     *
     * @param alias        file alias
     * @param filename     file name
     * @param fileResource file resource
     * @throws IOException        if file not exists
     * @throws DuplicateException if duplicate sql fragment name found in 1 sql file
     * @throws URISyntaxException if file uri syntax error
     */
    protected void putResource(String alias, String filename, FileResource fileResource) throws IOException, URISyntaxException {
        resources.put(alias, parse(alias, filename, fileResource));
    }

    /**
     * Parse sql file to structured resource.
     *
     * @param alias        file alias
     * @param filename     file name
     * @param fileResource file resource
     * @return structured resource
     * @throws IOException        if file not exists
     * @throws DuplicateException if duplicate sql fragment name found in same sql file
     * @throws URISyntaxException if file uri syntax error
     */
    public Resource parse(String alias, String filename, FileResource fileResource) throws IOException, URISyntaxException {
        Map<String, Sql> entry = new LinkedHashMap<>();
        String xqlDesc = "";
        try (BufferedReader reader = fileResource.getBufferedReader(Charset.forName(charset))) {
            String blockName = "";
            List<String> descriptionBuffer = new ArrayList<>();
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
                if (trimLine.startsWith(ANNO_START)) {
                    if (trimLine.startsWith(SQL_DESC_START)) {
                        if (trimLine.endsWith(SQL_DESC_END)) {
                            String description = trimLine.substring(3, trimLine.length() - 3);
                            if (!description.trim().isEmpty()) {
                                descriptionBuffer.add(description);
                            }
                            continue;
                        }
                        String descriptionStart = trimLine.substring(3);
                        if (!descriptionStart.trim().isEmpty()) {
                            descriptionBuffer.add(descriptionStart);
                        }
                        String descLine;
                        while ((descLine = reader.readLine()) != null) {
                            if (descLine.trim().endsWith(SQL_DESC_END)) {
                                String descriptionEnd = descLine.substring(0, descLine.lastIndexOf(SQL_DESC_END));
                                if (!descriptionEnd.trim().isEmpty()) {
                                    descriptionBuffer.add(descriptionEnd);
                                }
                                break;
                            }
                            descriptionBuffer.add(descLine);
                        }
                        continue;
                    }
                    if (xqlDesc.isEmpty()) {
                        StringJoiner descSb = new StringJoiner(NEW_LINE);
                        boolean isDesc = false;
                        String annoLine;
                        while ((annoLine = reader.readLine()) != null) {
                            String trimAnnoLine = annoLine.trim();
                            if (trimAnnoLine.equals(XQL_DESC_QUOTE)) {
                                isDesc = !isDesc;
                                continue;
                            }
                            if (isDesc) {
                                descSb.add(trimAnnoLine);
                            }
                            if (trimAnnoLine.endsWith(ANNO_END)) {
                                break;
                            }
                        }
                        xqlDesc = descSb.toString();
                    }
                }
                // exclude single line annotation except expression keywords
                if (!trimLine.startsWith("--") || (StringUtil.startsWithsIgnoreCase(trimAnnotation(trimLine), TAGS))) {
                    if (!blockName.isEmpty()) {
                        sqlBodyBuffer.add(line);
                        if (trimLine.endsWith(delimiter)) {
                            String sql = removeBlockAnnotation(String.join(NEW_LINE, sqlBodyBuffer));
                            sql = sql.substring(0, sql.lastIndexOf(delimiter)).trim();
                            String description = String.join(NEW_LINE, descriptionBuffer);
                            Sql sqlObj = new Sql(sql);
                            sqlObj.setDescription(description);
                            entry.put(blockName, sqlObj);
                            log.debug("scan {} to get sql({}) [{}.{}]：{}", filename, delimiter, alias, blockName, SqlHighlighter.highlightIfAnsiCapable(sql));
                            blockName = "";
                            sqlBodyBuffer.clear();
                            descriptionBuffer.clear();
                        }
                    }
                }
            }
            // if last part of sql is not ends with delimiter symbol
            if (!blockName.isEmpty()) {
                String lastSql = removeBlockAnnotation(String.join(NEW_LINE, sqlBodyBuffer));
                String lastDesc = String.join(NEW_LINE, descriptionBuffer);
                Sql sqlObj = new Sql(lastSql);
                sqlObj.setDescription(lastDesc);
                entry.put(blockName, sqlObj);
                log.debug("scan {} to get sql({}) [{}.{}]：{}", filename, delimiter, alias, blockName, SqlHighlighter.highlightIfAnsiCapable(lastSql));
            }
        }
        if (!entry.isEmpty()) {
            mergeSqlTemplate(entry);
        }
        Resource resource = new Resource(alias, filename);
        resource.setEntry(Collections.unmodifiableMap(entry));
        resource.setLastModified(fileResource.getLastModified());
        resource.setDescription(xqlDesc);
        return resource;
    }

    /**
     * Merge and reuse sql template into sql fragment.
     *
     * @param sqlResource sql resource
     */
    protected void mergeSqlTemplate(Map<String, Sql> sqlResource) {
        Map<String, String> templates = new HashMap<>();
        for (Map.Entry<String, Sql> e : sqlResource.entrySet()) {
            String k = e.getKey();
            if (k.startsWith("${")) {
                String template = fixTemplate(e.getValue().getContent());
                templates.put(k.substring(2, k.length() - 1), template);
            }
        }
        if (templates.isEmpty() && constants.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Sql> e : sqlResource.entrySet()) {
            Sql sql = e.getValue();
            String sqlContent = sql.getContent();
            if (sqlContent.contains("${")) {
                sqlContent = SqlUtil.formatSql(sqlContent, templates);
                sqlContent = SqlUtil.formatSql(sqlContent, constants);
                // remove empty line.
                sql.setContent(sqlContent.replaceAll("\\s*\r?\n", NEW_LINE));
            }
        }
    }

    /**
     * In case line annotation in template occurs error,
     * e.g.
     * <p>sql statement:</p>
     * <blockquote>
     * <pre>select * from test.user where ${cnd} order by id</pre>
     * </blockquote>
     * <p>{cnd}</p>
     * <blockquote>
     * <pre>
     * -- #if :id &lt;&gt; blank
     *    id = :id
     * -- #fi
     * </pre>
     * </blockquote>
     * <p>result:</p>
     * <blockquote>
     * <pre>
     * select * from test.user where
     * -- #if :id &lt;&gt; blank
     *    id = :id
     * -- #fi
     * order by id
     *     </pre>
     * </blockquote>
     *
     * @param template template
     * @return safe template
     */
    protected String fixTemplate(String template) {
        String newTemplate = template;
        if (newTemplate.trim().startsWith("--")) {
            newTemplate = NEW_LINE + newTemplate;
        }
        int lastLN = newTemplate.lastIndexOf(NEW_LINE);
        if (lastLN != -1) {
            String lastLine = newTemplate.substring(lastLN);
            if (lastLine.trim().startsWith("--")) {
                newTemplate += NEW_LINE;
            }
        }
        return newTemplate;
    }

    /**
     * Load all sql files and parse to structured resources.
     *
     * @throws UncheckedIOException if file not exists or read error
     * @throws RuntimeException     if uri syntax error
     */
    protected void loadResources() {
        try {
            // In case method copyStateTo invoked, files are updated but resources not,
            // It's necessary to remove non-associated dirty resources.
            resources.entrySet().removeIf(e -> !files.containsKey(e.getKey()));
            // Reload and parse all sql file.
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
     * Load custom pipes.
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
     * Initialing XQL file manager.
     *
     * @throws UncheckedIOException if file not exists or read error
     * @throws RuntimeException     if sql uri syntax error or load pipes error
     * @throws DuplicateException   if duplicate sql fragment name found in same sql file
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
     * Foreach all sql resource.
     *
     * @param consumer (alias, resource) -&gt; void
     */
    public void foreach(BiConsumer<String, Resource> consumer) {
        resources.forEach(consumer);
    }

    /**
     * Get all sql fragment name.
     *
     * @return sql fragment names set
     */
    public Set<String> names() {
        Set<String> names = new HashSet<>();
        foreach((k, r) -> r.getEntry().keySet().forEach(n -> names.add(k + "." + n)));
        return names;
    }

    /**
     * Get sql fragment count.
     *
     * @return sql fragment count
     */
    public int size() {
        int i = 0;
        for (Resource resource : resources.values()) {
            i += resource.getEntry().size();
        }
        return i;
    }

    /**
     * Check resources contains sql fragment or not.
     *
     * @param name sql reference name ({@code <alias>.<sqlName>})
     * @return true if exists or false
     */
    public boolean contains(String name) {
        int dotIdx = name.lastIndexOf(".");
        if (dotIdx < 1) {
            throw new IllegalArgumentException("Invalid sql reference name, please follow <alias>.<sqlName> format.");
        }
        String alias = name.substring(0, dotIdx);
        if (resources.containsKey(alias)) {
            String sqlName = name.substring(dotIdx + 1);
            return getResource(alias).getEntry().containsKey(sqlName);
        }
        return false;
    }

    /**
     * Get sql resource.
     *
     * @param alias sql file alias
     * @return sql resource
     */
    public Resource getResource(String alias) {
        return resources.get(alias);
    }

    /**
     * Get all sql resources.
     *
     * @return unmodifiable sql resources
     */
    public Map<String, Resource> getResources() {
        return Collections.unmodifiableMap(resources);
    }

    /**
     * Get a sql object.
     *
     * @param name sql reference name ({@code <alias>.<sqlName>})
     * @return sql object
     * @throws NoSuchElementException   if sql fragment name not exists
     * @throws IllegalArgumentException if sql reference name format error
     */
    public Sql getSqlObject(String name) {
        int dotIdx = name.lastIndexOf(".");
        if (dotIdx < 1) {
            throw new IllegalArgumentException("Invalid sql reference name, please follow <alias>.<sqlName> format.");
        }
        String alias = name.substring(0, dotIdx);
        if (resources.containsKey(alias)) {
            Map<String, Sql> singleResource = getResource(alias).getEntry();
            String sqlName = name.substring(dotIdx + 1);
            if (singleResource.containsKey(sqlName)) {
                return singleResource.get(sqlName);
            }
        }
        throw new NoSuchElementException(String.format("no SQL named [%s] was found.", name));
    }

    /**
     * Get a sql fragment.
     *
     * @param name sql reference name ({@code <alias>.<sqlName>})
     * @return sql fragment
     * @throws NoSuchElementException   if sql fragment name not exists
     * @throws IllegalArgumentException if sql reference name format error
     */
    public String get(String name) {
        String sql = getSqlObject(name).getContent();
        return SqlUtil.trimEnd(sql);
    }

    /**
     * Get and calc a dynamic sql.
     *
     * @param name sql name ({@code <alias>.<sqlName>})
     * @param args dynamic sql script expression args
     * @return parsed sql and extra args calculated by {@code #for} expression if exists
     * @throws NoSuchElementException if sql fragment name not exists
     * @throws ScriptSyntaxException  dynamic sql script syntax error
     * @see DynamicSqlParser
     */
    public Pair<String, Map<String, Object>> get(String name, Map<String, ?> args) {
        try {
            return parseDynamicSql(get(name), args);
        } catch (Exception e) {
            throw new ScriptSyntaxException("an error occurred when getting dynamic sql of name: " + name, e);
        }
    }

    /**
     * Parse dynamic sql.
     *
     * @param sql  dynamic sql
     * @param args dynamic sql script expression args
     * @return parsed sql and extra args calculated by {@code #for} expression if exists
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
        String fixedSql = SqlUtil.repairSyntaxError(parsedSql);
        return Pair.of(fixedSql, parser.getForContextVars());
    }

    /**
     * Get a constant.
     *
     * @param key constant name
     * @return constant value
     */
    public Object getConstant(String key) {
        return constants.get(key);
    }

    /**
     * Check XQL file manager is initialized or not.
     *
     * @return true if initialized or false
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Close and cleanup XQL file manager.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            files.clear();
            resources.clear();
            pipes.clear();
            pipeInstances.clear();
            constants.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Trim line annotation for detect dynamic sql script expression.
     *
     * @param line current line
     * @return script expression or other line
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
     * Create a new dynamic sql parser.
     *
     * @return dynamic sql parser
     */
    public DynamicSqlParser newDynamicSqlParser() {
        return new DynamicSqlParser();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof XQLFileManager)) return false;
        if (!super.equals(o)) return false;

        XQLFileManager that = (XQLFileManager) o;

        return getResources().equals(that.getResources());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getResources().hashCode();
        return result;
    }

    /**
     * Dynamic sql parser.
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
         * Cleanup annotation in for loop and create indexed arg for special format named arg, e.g.
         * <p>Mock data {@code users} and {@code forIndex: 0}:</p>
         * <blockquote>
         * <pre>["CYX", "jack", "Mike"]</pre>
         * </blockquote>
         * <p>for loop:</p>
         * <blockquote>
         * <pre>
         * -- #for user,idx of :users delimiter ', '
         *    :_for.user
         * -- #done
         * </pre>
         * </blockquote>
         * <p>result:</p>
         * <blockquote>
         * <pre>
         * :_for.user_0_0,
         * :_for.user_0_1,
         * :_for.user_0_2,
         * </pre>
         * </blockquote>
         *
         * @param forIndex each for loop auto index
         * @param varIndex for var auto index
         * @param varName  for var name,  e.g. {@code <user>}
         * @param idxName  for index name,  e.g. {@code <idx>}
         * @param body     content in for loop
         * @param args     each for loop args (index and value) which created by for expression
         * @return formatted content
         */
        @Override
        protected String forLoopBodyFormatter(int forIndex, int varIndex, String varName, String idxName, List<String> body, Map<String, Object> args) {
            body.removeIf(l -> {
                String tl = l.trim();
                return tl.startsWith("--") && !tl.substring(2).trim().startsWith("#");
            });
            String formatted = SqlUtil.formatSql(String.join(NEW_LINE, body), args);
            if (Objects.nonNull(varName)) {
                String varParam = VAR_PREFIX + forVarKey(varName, forIndex, varIndex);
                formatted = formatted.replace(VAR_PREFIX + varName, varParam);
            }
            if (Objects.nonNull(idxName)) {
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
     * Sql object.
     */
    public static final class Sql {
        private String content;
        private String description = "";

        public Sql(String content) {
            this.content = content;
        }

        void setContent(String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }

        public String getDescription() {
            return description;
        }

        void setDescription(String description) {
            this.description = description;
        }
    }

    /**
     * Sql file resource.
     */
    public static final class Resource {
        private final String alias;
        private final String filename;
        private long lastModified = -1;
        private String description;
        private Map<String, Sql> entry;

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

        public String getDescription() {
            return description;
        }

        void setDescription(String description) {
            this.description = description;
        }

        public Map<String, Sql> getEntry() {
            return entry;
        }

        void setEntry(Map<String, Sql> entry) {
            if (Objects.nonNull(entry))
                this.entry = entry;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Resource)) return false;

            Resource resource = (Resource) o;
            return getLastModified() == resource.getLastModified() && Objects.equals(getAlias(), resource.getAlias()) && Objects.equals(getFilename(), resource.getFilename()) && Objects.equals(getDescription(), resource.getDescription()) && getEntry().equals(resource.getEntry());
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(getAlias());
            result = 31 * result + Objects.hashCode(getFilename());
            result = 31 * result + Long.hashCode(getLastModified());
            result = 31 * result + Objects.hashCode(getDescription());
            result = 31 * result + getEntry().hashCode();
            return result;
        }
    }
}
