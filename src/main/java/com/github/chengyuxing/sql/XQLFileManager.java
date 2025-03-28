package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.lexer.FlowControlLexer;
import com.github.chengyuxing.common.script.parser.FlowControlParser;
import com.github.chengyuxing.common.script.exception.ScriptSyntaxException;
import com.github.chengyuxing.common.script.expression.IPipe;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.utils.ObjectUtil;
import com.github.chengyuxing.common.utils.ReflectUtil;
import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.DuplicateException;
import com.github.chengyuxing.sql.plugins.TemplateFormatter;
import com.github.chengyuxing.sql.utils.SqlGenerator;
import com.github.chengyuxing.sql.utils.SqlHighlighter;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;
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

import static com.github.chengyuxing.common.utils.StringUtil.NEW_LINE;
import static com.github.chengyuxing.common.utils.StringUtil.containsAnyIgnoreCase;

/**
 * <h2>Dynamic SQL File Manager</h2>
 * <p>Use standard sql block annotation ({@code /**}{@code /}), line annotation ({@code --}),
 * named parameter ({@code :name}) and string template variable ({@code ${template}}) to
 * extends SQL file standard syntax with special content format, brings more features to
 * SQL file and follow the strict SQL file syntax.</p>
 * <p>File type supports: {@code .xql}, {@code .sql}, suffix {@code .xql} means this file is
 * {@code XQLFileManager} default file type.</p>
 *
 * <p>File path support {@link FileResource URI and classpath}, {@code .yml} configuration file support
 * environment variable ({@link System#getenv()}) e.g. {@link #YML} :</p>
 * <blockquote><pre>
 * files:
 *   home: {@code https://server/home.xql?token=${env.TOKEN}}
 *   foo: xlqs/a.xql
 *   bar: !path [xqls, b.xql]
 * </pre></blockquote>
 *
 * <p>Notice: <a href="https://plugins.jetbrains.com/plugin/21403-rabbit-sql/introduction/execute-dynamic-sql">
 * Rabbit-SQL IDEA Plugin</a> only support detect {@code .xql} file.</p>
 *
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
 * {@linkplain DynamicSqlParser Dynamic sql script} write in line annotation where starts with {@code --},
 * check example following class path file: {@code home.xql.template}.
 * <p>Invoke method {@link #get(String, Map)} to enjoy the dynamic sql!</p>
 *
 * @see FlowControlParser
 */
public class XQLFileManager extends XQLFileManagerConfig implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    public static final Pattern KEY_PATTERN = Pattern.compile("/\\*\\s*(\\[\\s*(?<sqlName>[^\\s\\[\\]{}]+)\\s*]|\\{\\s*(?<partName>[^\\s\\[\\]{}]+)\\s*})\\s*\\*/");
    public static final String SQL_DESC_START = "/*#";
    public static final String XQL_DESC_QUOTE = "@@@";
    public static final String YML = "xql-file-manager.yml";
    /**
     * Template ({@code ${key}}) formatter.
     * Default implementation: {@link SqlUtil#parseValue(Object, boolean) parseValue(value, boolean)}
     */
    private TemplateFormatter templateFormatter = SqlUtil::parseValue;
    private final ClassLoader classLoader = this.getClass().getClassLoader();
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, IPipe<?>> pipeInstances = new HashMap<>();
    private volatile boolean initialized;

    /**
     * Constructs a new XQLFileManager.
     *
     * @see XQLFileManagerConfig
     */
    public XQLFileManager() {
    }

    /**
     * Constructs a new XQLFileManager with config location.
     *
     * @param configLocation {@link FileResource config file}: supports {@code .yml} and {@code .properties}
     * @see XQLFileManagerConfig
     */
    public XQLFileManager(@NotNull String configLocation) {
        super(configLocation);
    }

    /**
     * Constructs a new XQLFileManager with xql file manager config.
     *
     * @param config xql file manager config
     */
    public XQLFileManager(@NotNull XQLFileManagerConfig config) {
        config.copyStateTo(this);
    }

    /**
     * Add a sql file.
     *
     * @param alias    file alias
     * @param fileName file path name
     */
    public void add(@NotNull String alias, @NotNull String fileName) {
        if (files.containsKey(alias)) {
            throw new DuplicateException("duplicate alias: " + alias);
        }
        files.put(alias, fileName);
    }

    /**
     * Add a sql file with default alias(file name without extension).
     *
     * @param fileName file path name
     * @return file alias
     */
    public String add(@NotNull String fileName) {
        String alias = FileResource.getFileName(fileName, false);
        add(alias, fileName);
        return alias;
    }

    /**
     * Remove a sql file with associated sql resource.
     *
     * @param alias file alias
     */
    public void remove(@NotNull String alias) {
        files.remove(alias);
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
    public @NotNull Resource parse(@NotNull String alias, @NotNull String filename, @NotNull FileResource fileResource) throws IOException, URISyntaxException {
        Map<String, Sql> entry = new LinkedHashMap<>();
        var xqlDesc = new StringJoiner(NEW_LINE);
        try (BufferedReader reader = fileResource.getBufferedReader(Charset.forName(charset))) {
            String line;
            String currentName = null;
            var sqlBuffer = new StringBuilder();
            var descriptionBuffer = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (trimLine.isEmpty()) continue;
                Matcher matcher = KEY_PATTERN.matcher(trimLine);
                if (matcher.matches()) {
                    String sqlName = matcher.group("sqlName");
                    String partName = matcher.group("partName");
                    String name = sqlName != null ? sqlName : "${" + partName + "}";
                    if (sqlBuffer.length() != 0) {
                        throw new IllegalStateException("the sql which before the name '" + ObjectUtil.coalesce(sqlName, partName) + "' does not seem to end with the '" + delimiter + "' in " + filename);
                    }
                    if (entry.containsKey(name)) {
                        throw new DuplicateException("duplicate name: '" + ObjectUtil.coalesce(sqlName, partName) + "' in " + filename);
                    }
                    currentName = name;
                    continue;
                }
                if (trimLine.startsWith("/*")) {
                    // /*#...#*/
                    if (trimLine.startsWith(SQL_DESC_START)) {
                        if (trimLine.endsWith("*/")) {
                            String description = trimLine.substring(3, trimLine.length() - 2);
                            if (description.endsWith("#")) {
                                description = description.substring(0, description.length() - 1);
                            }
                            if (!description.trim().isEmpty()) {
                                descriptionBuffer.append(description).append(NEW_LINE);
                            }
                            continue;
                        }
                        String descriptionStart = trimLine.substring(3);
                        if (!descriptionStart.trim().isEmpty()) {
                            descriptionBuffer.append(descriptionStart).append(NEW_LINE);
                        }
                        String descLine;
                        while ((descLine = reader.readLine()) != null) {
                            if (descLine.trim().endsWith("*/")) {
                                String descriptionEnd = descLine.substring(0, descLine.lastIndexOf("*/"));
                                if (descriptionEnd.endsWith("#")) {
                                    descriptionEnd = descriptionEnd.substring(0, descriptionEnd.length() - 1);
                                }
                                if (!descriptionEnd.trim().isEmpty()) {
                                    descriptionBuffer.append(descriptionEnd).append(NEW_LINE);
                                }
                                break;
                            }
                            descriptionBuffer.append(descLine).append(NEW_LINE);
                        }
                        continue;
                    }
                    // @@@
                    // ...
                    // @@@
                    if (entry.isEmpty()) {
                        if (trimLine.endsWith("*/")) {
                            continue;
                        }
                        String a;
                        descBlock:
                        while ((a = reader.readLine()) != null) {
                            String ta = a.trim();
                            if (ta.endsWith("*/")) {
                                break;
                            }
                            if (ta.equals(XQL_DESC_QUOTE)) {
                                String b;
                                while ((b = reader.readLine()) != null) {
                                    String tb = b.trim();
                                    if (tb.equals(XQL_DESC_QUOTE)) {
                                        break;
                                    }
                                    if (tb.endsWith("*/")) {
                                        break descBlock;
                                    }
                                    xqlDesc.add(tb);
                                }
                            }
                        }
                    }
                }
                if (currentName != null) {
                    sqlBuffer.append(line).append(NEW_LINE);
                    if (trimLine.endsWith(delimiter)) {
                        String sql = sqlBuffer.toString().trim().replaceAll(delimiter + "$", "");
                        String desc = descriptionBuffer.toString().trim();
                        entry.put(currentName, scanSql(alias, filename, currentName, sql, desc));
                        currentName = null;
                        sqlBuffer.setLength(0);
                        descriptionBuffer.setLength(0);
                    }
                }
            }
            // if last part of sql is not ends with delimiter symbol
            if (currentName != null) {
                String lastSql = sqlBuffer.toString().trim().replaceAll(delimiter + "$", "");
                String lastDesc = descriptionBuffer.toString().trim();
                entry.put(currentName, scanSql(alias, filename, currentName, lastSql, lastDesc));
            }
        }
        if (!entry.isEmpty()) {
            mergeSqlTemplate(entry);
        }
        Resource resource = new Resource(filename);
        resource.setEntry(Collections.unmodifiableMap(entry));
        resource.setLastModified(fileResource.getLastModified());
        resource.setDescription(xqlDesc.toString().trim());
        return resource;
    }

    /**
     * Scan sql object.
     *
     * @param alias    alias
     * @param filename sql file name
     * @param sqlName  sql fragment name
     * @param sql      sql content
     * @param desc     sql description
     * @return sql object
     */
    protected Sql scanSql(String alias, String filename, String sqlName, String sql, String desc) {
        try {
            newDynamicSqlParser(sql).verify();
        } catch (ScriptSyntaxException e) {
            throw new ScriptSyntaxException("File: " + filename + " -> '" + sqlName + "' dynamic sql script syntax error.", e);
        }
        Sql sqlObj = new Sql(sql);
        sqlObj.setDescription(desc);
        log.debug("scan({}) {} to get sql [{}.{}]: {}", delimiter, filename, alias, sqlName, SqlHighlighter.highlightIfAnsiCapable(sql));
        return sqlObj;
    }

    /**
     * <p>Merge and reuse sql template into sql fragment.</p>
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
     * @param sqlResource sql resource
     */
    protected void mergeSqlTemplate(Map<String, Sql> sqlResource) {
        Map<String, String> templates = new HashMap<>();
        for (Map.Entry<String, Sql> e : sqlResource.entrySet()) {
            String k = e.getKey();
            if (k.startsWith("${")) {
                String template = e.getValue().getContent();
                // fix template
                if (template.trim().startsWith("--")) {
                    template = NEW_LINE + template;
                }
                int lastLN = template.lastIndexOf(NEW_LINE);
                if (lastLN != -1) {
                    String lastLine = template.substring(lastLN);
                    if (lastLine.trim().startsWith("--")) {
                        template += NEW_LINE;
                    }
                }
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
                sqlContent = SqlUtil.formatSql(sqlContent, templates, templateFormatter);
                sqlContent = SqlUtil.formatSql(sqlContent, constants, templateFormatter);
                // remove empty line.
                sql.setContent(StringUtil.removeEmptyLine(sqlContent));
            }
        }
    }

    /**
     * Load all sql files and parse to structured resources.
     *
     * @throws UncheckedIOException if file not exists or read error
     * @throws RuntimeException     if uri syntax error
     */
    protected void loadResources() {
        try {
            for (Map.Entry<String, String> e : files.entrySet()) {
                String alias = e.getKey();
                String filename = e.getValue();
                Resource resource = files.getResource(alias);
                FileResource fr = new FileResource(filename);
                if (fr.exists()) {
                    String ext = fr.getFilenameExtension();
                    if (ext != null && (ext.equals("sql") || ext.equals("xql"))) {
                        long oldLastModified = resource.getLastModified();
                        long lastModified = fr.getLastModified();
                        if (oldLastModified > 0 && oldLastModified == lastModified) {
                            log.debug("skip load unmodified resource [{}] from [{}]", alias, filename);
                            continue;
                        }
                        Resource parsed = parse(alias, filename, fr);
                        resource.setEntry(parsed.getEntry());
                        resource.setDescription(parsed.getDescription());
                        resource.setLastModified(parsed.getLastModified());
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
        if (pipes.keySet().equals(pipeInstances.keySet())) {
            return;
        }
        try {
            for (var entry : pipes.entrySet()) {
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
     * Decode sql reference to alias and sql name.
     *
     * @param sqlReference sql reference name ({@code <alias>.<sqlName>})
     * @return alias and sql name
     */
    public static @NotNull Pair<String, String> decodeSqlReference(@NotNull String sqlReference) {
        int dotIdx = sqlReference.lastIndexOf(".");
        if (dotIdx < 1) {
            throw new IllegalArgumentException("Invalid sql reference name, please follow <alias>.<sqlName> format.");
        }
        String alias = sqlReference.substring(0, dotIdx);
        String name = sqlReference.substring(dotIdx + 1);
        return Pair.of(alias, name);
    }

    /**
     * Encode alias and sql name to sql reference name.
     *
     * @param alias xql file alias
     * @param name  sql name
     * @return sql reference name
     */
    public static @NotNull String encodeSqlReference(@NotNull String alias, @NotNull String name) {
        return alias + "." + name;
    }

    /**
     * Foreach all sql resource.
     *
     * @param consumer (alias, resource) -&gt; void
     */
    public void foreach(BiConsumer<String, Resource> consumer) {
        files.getResources().forEach(consumer);
    }

    /**
     * Get all sql fragment name.
     *
     * @return sql fragment names set
     */
    public Set<String> names() {
        Set<String> names = new HashSet<>();
        foreach((k, r) -> r.getEntry().keySet().forEach(n -> names.add(encodeSqlReference(k, n))));
        return names;
    }

    /**
     * Get sql fragment count.
     *
     * @return sql fragment count
     */
    public int size() {
        int i = 0;
        for (Resource resource : files.getResources().values()) {
            i += resource.getEntry().size();
        }
        return i;
    }

    /**
     * Get sql resource.
     *
     * @param alias sql file alias
     * @return sql resource
     */
    public Resource getResource(String alias) {
        return files.getResource(alias);
    }

    /**
     * Get all sql resources.
     *
     * @return unmodifiable sql resources
     */
    public @NotNull @Unmodifiable Map<String, Resource> getResources() {
        return files.getResources();
    }

    /**
     * Check resources contains sql fragment or not.
     *
     * @param name sql reference name ({@code <alias>.<sqlName>})
     * @return true if exists or false
     */
    public boolean contains(String name) {
        if (Objects.isNull(name)) return false;
        Pair<String, String> p = decodeSqlReference(name);
        Resource resource = files.getResources().get(p.getItem1());
        if (Objects.isNull(resource)) {
            return false;
        }
        return resource.getEntry().containsKey(p.getItem2());
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
        if (Objects.isNull(name)) {
            throw new IllegalArgumentException("sql object name is null");
        }
        var p = decodeSqlReference(name);
        var resource = getResource(p.getItem1());
        if (Objects.isNull(resource)) {
            throw new NoSuchElementException(String.format("Resource with alias [%s] not found.", p.getItem1()));
        }
        var sql = resource.getEntry().get(p.getItem2());
        if (Objects.isNull(sql)) {
            throw new NoSuchElementException(String.format("no SQL named [%s] was found.", p.getItem2()));
        }
        return sql;
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
    public Pair<String, Map<String, Object>> parseDynamicSql(@NotNull String sql, @Nullable Map<String, ?> args) {
        if (!containsAnyIgnoreCase(sql, FlowControlLexer.KEYWORDS)) {
            return Pair.of(sql, Collections.emptyMap());
        }
        Map<String, Object> myArgs = new HashMap<>();
        if (Objects.nonNull(args)) {
            myArgs.putAll(args);
        }
        myArgs.put("_parameter", args);
        myArgs.put("_databaseId", databaseId);
        var parser = newDynamicSqlParser(sql);
        var parsedSql = parser.parse(myArgs);
        return Pair.of(parsedSql, parser.getForContextVars());
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
     * Cleanup XQL file manager.
     */
    @Override
    public void close() {
        files.clear();
        pipes.clear();
        pipeInstances.clear();
        constants.clear();
    }

    /**
     * Create a new dynamic sql parser.
     *
     * @param sql sql
     * @return dynamic sql parser
     */
    public DynamicSqlParser newDynamicSqlParser(@NotNull String sql) {
        return new DynamicSqlParser(sql);
    }

    public TemplateFormatter getTemplateFormatter() {
        return templateFormatter;
    }

    public void setTemplateFormatter(@NotNull TemplateFormatter templateFormatter) {
        this.templateFormatter = templateFormatter;
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
    public class DynamicSqlParser extends FlowControlParser {
        public static final String FOR_VARS_KEY = "_for";
        public static final String VAR_PREFIX = FOR_VARS_KEY + '.';

        public DynamicSqlParser(@NotNull String sql) {
            super(sql);
        }

        @Override
        protected Map<String, IPipe<?>> getPipes() {
            return pipeInstances;
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
         *    :user
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
        protected String forLoopBodyFormatter(int forIndex, int varIndex, String varName, String idxName, String body, Map<String, Object> args) {
            String formatted = body;
            if (body.contains("${")) {
                formatted = SqlUtil.formatSql(body, args, templateFormatter);
            }
            if (formatted.contains(namedParamPrefix + varName) || formatted.contains(namedParamPrefix + idxName)) {
                StringBuilder sb = new StringBuilder();
                Pattern p = new SqlGenerator(namedParamPrefix).getNamedParamPattern();
                Matcher m = p.matcher(formatted);
                int lastMatchEnd = 0;
                while (m.find()) {
                    sb.append(formatted, lastMatchEnd, m.start());
                    lastMatchEnd = m.end();
                    String name = m.group(1);
                    if (Objects.isNull(name)) {
                        sb.append(m.group());
                        continue;
                    }
                    if (name.equals(varName) || name.equals(idxName)) {
                        sb.append(namedParamPrefix)
                                .append(VAR_PREFIX)
                                .append(forVarKey(name, forIndex, varIndex));
                        continue;
                    }
                    // -- #for item of :data | kv
                    //  ${item.key} = :item.value
                    //-- #done
                    // --------------------------
                    // name: item.value
                    // varName: item
                    if (name.startsWith(varName + '.')) {
                        String suffix = name.substring(varName.length());
                        sb.append(namedParamPrefix)
                                .append(VAR_PREFIX)
                                .append(forVarKey(varName, forIndex, varIndex))
                                .append(suffix);
                        continue;
                    }
                    sb.append(m.group());
                }
                sb.append(formatted.substring(lastMatchEnd));
                formatted = sb.toString();
            }
            return formatted;
        }

        /**
         * Trim line annotation for detect dynamic sql script expression.
         *
         * @param line current line
         * @return script expression or other line
         */
        @Override
        protected String trimExpressionLine(String line) {
            String lt = line.trim();
            if (lt.startsWith("--")) {
                return lt.substring(2);
            }
            return line;
        }
    }
}
