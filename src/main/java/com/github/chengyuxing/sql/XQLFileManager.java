package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.RabbitScriptEngine;
import com.github.chengyuxing.common.script.ast.ScriptAst;
import com.github.chengyuxing.common.script.ast.ScriptEngine;
import com.github.chengyuxing.common.script.ast.impl.EvalContext;
import com.github.chengyuxing.common.script.ast.impl.EvalResult;
import com.github.chengyuxing.common.script.ast.impl.VarMeta;
import com.github.chengyuxing.common.script.lang.Directives;
import com.github.chengyuxing.common.script.exception.ScriptSyntaxException;
import com.github.chengyuxing.common.script.pipe.IPipe;
import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.util.ValueUtils;
import com.github.chengyuxing.common.util.ReflectUtils;
import com.github.chengyuxing.common.util.StringUtils;
import com.github.chengyuxing.sql.exceptions.XQLParseException;
import com.github.chengyuxing.sql.util.SqlGenerator;
import com.github.chengyuxing.sql.util.SqlHighlighter;
import com.github.chengyuxing.sql.util.SqlUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
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

import static com.github.chengyuxing.common.util.StringUtils.NEW_LINE;

/**
 * <h2>Dynamic SQL File Manager</h2>
 * <p>Use standard SQL block comment ({@code /**}{@code /}), line comment ({@code --}),
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
 *   foo: xqls/a.xql
 *   bar: !path [xqls, b.xql]
 * </pre></blockquote>
 *
 * <p>Notice: <a href="https://plugins.jetbrains.com/plugin/21403-rabbit-sql/introduction/execute-dynamic-sql">
 * Rabbit-SQL IDEA Plugin</a> only support detect {@code .xql} file.</p>
 *
 * <h3>File content structure</h3>
 * <p>The xql object is {@code key-value} format, key is sql name, value is sql statement,
 * every xql object is delimited by single symbol {@code ;} at the end.</p>
 * <p>For some statement blocks that contain internal {@code ;} (such as PostgreSQL's {@code DO $$...$$} or PLSQL procedure body)
 * A comment marker {@code --} can be used at the end of a sentence line to prevent premature truncation, e.g.</p>
 * <blockquote>
 * <pre>
 * /&#42;[queryList]&#42;/
 * select * from guest where
 *  -- //TEMPLATE-BEGIN:myInLineCnd
 *  -- #if :id != blank
 *     id = :id
 *  -- #fi
 *  -- //TEMPLATE-END
 * ${order}
 * ;
 *
 * /&#42;[queryCount]&#42;/
 * select count(*) from guest where ${myInLineCnd};
 *
 * /&#42;{order}&#42;/
 * order by id desc;
 *
 * /&#42;[sqlNameN]&#42;/
 * select * from test.user;
 *
 * /&#42;#This is a description comment mark.#&#42;/
 * /&#42;[plsql]&#42;/
 * /&#42;#This is a plsql statement block!#&#42;/
 * begin;--
 *     select ... ;--
 * end;--
 * ;
 * </pre>
 * </blockquote>
 * <p>
 * {@linkplain RabbitScriptEngine Dynamic sql script} write in line comment where starts with {@code --},
 * check example following class path file: {@code home.xql.template}.
 * <p>Supported Directives: {@link Directives Directives}</p>
 * <p>Invoke method {@link #get(String, Map)} to enjoy the dynamic SQL!</p>
 *
 * @see RabbitScriptEngine
 */
public class XQLFileManager extends XQLFileManagerConfig implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    public static final Pattern KEY_PATTERN = Pattern.compile("/\\*\\s*(\\[\\s*(?<sqlName>[a-zA-Z_][\\w-]*)\\s*]|\\{\\s*(?<partName>[a-zA-Z_]\\w*)\\s*})\\s*\\*/");
    public static final Pattern INLINE_TEMPLATE_BEGIN_PATTERN = Pattern.compile("(?i)\\s*--\\s*//\\s*TEMPLATE-BEGIN\\s*:\\s*(?<key>[a-zA-Z_]\\w*)\\s*");
    public static final Pattern INLINE_TEMPLATE_END_PATTERN = Pattern.compile("(?i)\\s*--\\s*//\\s*TEMPLATE-END\\s*");
    public static final Pattern META_DATA_PATTERN = Pattern.compile("\\s*--\\s*@(?<name>[a-zA-Z]\\w+)\\s+(?<value>.+)\\s*");
    public static final String XQL_DESC_QUOTE = "@@@";
    public static final String YML = "xql-file-manager.yml";
    /**
     * Notice: function for normalizes the directive line by removing the leading '--' if present.
     */
    private final ScriptEngine scriptEngine = new RabbitScriptEngine(line -> {
        int idx = SqlUtils.indexOfWholeLineComment(line);
        if (idx != -1) {
            return line.substring(idx + 2);
        }
        return line;
    });
    private final ReentrantLock lock = new ReentrantLock();
    private SqlGenerator sqlGenerator = new SqlGenerator(DEFAULT_NAMED_PARAM_PREFIX);
    private volatile Map<String, Resource> resources = Collections.emptyMap();
    private volatile Map<String, IPipe<?>> pipeInstances = Collections.emptyMap();
    private volatile boolean loading;
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
     * Add a SQL file.
     *
     * @param alias    file alias
     * @param fileName file path name
     */
    public void add(@NotNull String alias, @NotNull String fileName) {
        if (getFiles().containsKey(alias)) {
            throw new IllegalArgumentException("Duplicate alias: " + alias);
        }
        getFiles().put(alias, fileName);
    }

    /**
     * Add a SQL file with default alias(file name without extension).
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
        getFiles().remove(alias);
    }

    /**
     * Parse sql file to structured resource.
     *
     * @param alias        file alias
     * @param filename     file name
     * @param fileResource file resource
     * @return structured resource
     * @throws IOException        if file not exists
     * @throws URISyntaxException if file uri syntax error
     */
    public @NotNull Resource parseXql(@NotNull String alias, @NotNull String filename, @NotNull FileResource fileResource) throws IOException, URISyntaxException {
        Map<String, Sql> entry = new LinkedHashMap<>();
        StringJoiner xqlFileDescriptionBuffer = new StringJoiner(NEW_LINE);
        try (BufferedReader reader = fileResource.getBufferedReader(Charset.forName(getCharset()))) {
            String line;
            String currentName = null;
            boolean isMainStarted = false;
            StringBuilder sqlBodyBuffer = new StringBuilder();
            StringBuilder sqlDescriptionBuffer = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (trimLine.isEmpty()) continue;
                Matcher matcher = KEY_PATTERN.matcher(trimLine);
                if (matcher.matches()) {
                    isMainStarted = true;
                    String sqlName = matcher.group("sqlName");
                    String partName = matcher.group("partName");
                    String name = sqlName != null ? sqlName : "${" + partName + "}";
                    if (sqlBodyBuffer.length() != 0) {
                        throw new XQLParseException("The sql which before the name '" + ValueUtils.coalesce(sqlName, partName) + "' does not seem to end with the ';' in " + filename);
                    }
                    if (entry.containsKey(name)) {
                        throw new XQLParseException("Duplicate name '" + ValueUtils.coalesce(sqlName, partName) + "' in " + filename);
                    }
                    currentName = name;
                    continue;
                }
                if (trimLine.startsWith("/*")) {
                    // /*#...#*/
                    if (trimLine.startsWith("/*#")) {
                        if (trimLine.endsWith("*/")) {
                            String description = trimLine.substring(3, trimLine.length() - 2);
                            if (description.endsWith("#")) {
                                description = description.substring(0, description.length() - 1);
                            }
                            if (!StringUtils.isBlank(description)) {
                                sqlDescriptionBuffer.append(description).append(NEW_LINE);
                            }
                            continue;
                        }
                        String descriptionStart = trimLine.substring(3);
                        if (!StringUtils.isBlank(descriptionStart)) {
                            sqlDescriptionBuffer.append(descriptionStart).append(NEW_LINE);
                        }
                        String descLine;
                        while ((descLine = reader.readLine()) != null) {
                            int endBlockIdx = StringUtils.lastIndexOfNonWhitespace(descLine, "*/");
                            if (endBlockIdx != -1) {
                                String descriptionEnd = descLine.substring(0, endBlockIdx);
                                if (descriptionEnd.endsWith("#")) {
                                    descriptionEnd = descriptionEnd.substring(0, descriptionEnd.length() - 1);
                                }
                                if (!StringUtils.isBlank(descriptionEnd)) {
                                    sqlDescriptionBuffer.append(descriptionEnd).append(NEW_LINE);
                                }
                                break;
                            }
                            sqlDescriptionBuffer.append(descLine).append(NEW_LINE);
                        }
                        continue;
                    }
                    // @@@
                    // ...
                    // @@@
                    if (!isMainStarted) {
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
                                    xqlFileDescriptionBuffer.add(tb);
                                }
                            }
                        }
                    }
                }
                if (currentName != null) {
                    sqlBodyBuffer.append(line).append(NEW_LINE);
                    if (trimLine.endsWith(";")) {
                        String sql = sqlBodyBuffer.toString().trim();
                        sql = sql.substring(0, sql.length() - 1).trim();
                        String desc = sqlDescriptionBuffer.toString().trim();

                        entry.put(currentName, scanSql(alias, filename, currentName, sql, desc));
                        appendInlineTemplate(alias, filename, entry, currentName, sql);

                        currentName = null;
                        sqlBodyBuffer.setLength(0);
                        sqlDescriptionBuffer.setLength(0);
                    }
                }
            }
            // if last part of SQL is not ends with ';' symbol
            if (currentName != null) {
                String lastSql = sqlBodyBuffer.toString().trim();
                String lastDesc = sqlDescriptionBuffer.toString().trim();

                entry.put(currentName, scanSql(alias, filename, currentName, lastSql, lastDesc));
                appendInlineTemplate(alias, filename, entry, currentName, lastSql);
            }
        }
        if (!entry.isEmpty()) {
            mergeSqlTemplate(entry);
        }
        Resource resource = new Resource(filename);
        resource.setEntry(Collections.unmodifiableMap(entry));
        resource.setLastModified(fileResource.getLastModified());
        resource.setDescription(xqlFileDescriptionBuffer.toString().trim());
        return resource;
    }

    /**
     * Scan sql object.
     *
     * @param alias    alias
     * @param filename SQL file name
     * @param sqlName  sql fragment name
     * @param sql      sql content
     * @param desc     sql description
     * @return sql object
     */
    protected @NotNull Sql scanSql(@NotNull String alias, @NotNull String filename, @NotNull String sqlName, @NotNull String sql, @NotNull String desc) {
        try {
            Sql sqlObj = new Sql(sql);
            sqlObj.setMetadata(parseMetadata(sql));
            sqlObj.setDescription(desc);
            log.debug("scan(;) {} to compile sql [{}.{}]: {}", filename, alias, sqlName, SqlHighlighter.highlightIfAnsiCapable(sql));
            return sqlObj;
        } catch (ScriptSyntaxException e) {
            throw new XQLParseException("File: " + filename + " -> '" + sqlName + "' has script syntax error", e);
        }
    }

    /**
     * Parse the SQL defined metadata: <code>-- @name value</code> .
     *
     * @param sql SQL
     * @return metadata
     */
    protected Map<String, String> parseMetadata(String sql) {
        Map<String, String> metadata = new HashMap<>();
        String[] lines = sql.split("\n");
        for (String line : lines) {
            Matcher m = META_DATA_PATTERN.matcher(line);
            if (m.find()) {
                metadata.put(m.group("name"), m.group("value"));
            } else {
                break;
            }
        }
        return metadata;
    }

    /**
     * Add the extracted inline template to entry.
     *
     * @param alias       alias
     * @param filename    SQL file name
     * @param entry       sql container
     * @param currentName sql name
     * @param sql         sql content
     */
    protected void appendInlineTemplate(@NotNull String alias, @NotNull String filename, Map<String, Sql> entry, String currentName, String sql) {
        extractInlineTemplate(sql, (key, template) -> {
            String name = "${" + key + "}";
            if (entry.containsKey(name)) {
                throw new XQLParseException("The template name '" + key + "' in SQL '" + currentName + "' has already been defined before.");
            }
            entry.put(name, scanSql(alias, filename, name, template, "Inline template extracted by '" + currentName + "'"));
        });
    }

    /**
     * Extract the inline template.
     *
     * @param sql      sql
     * @param consumer consumer the extracted template
     */
    protected void extractInlineTemplate(String sql, BiConsumer<String, String> consumer) {
        String[] lines = sql.split("\n");
        int i = 0;
        while (i < lines.length) {
            if (INLINE_TEMPLATE_END_PATTERN.matcher(lines[i]).matches()) {
                throw new XQLParseException("Inline template missing '//TEMPLATE-BEGIN:xxx' before the end at: " + i);
            }
            Matcher m = INLINE_TEMPLATE_BEGIN_PATTERN.matcher(lines[i]);
            if (m.matches()) {
                i++;
                StringJoiner sb = new StringJoiner("\n");
                while (!INLINE_TEMPLATE_END_PATTERN.matcher(lines[i]).matches()) {
                    if (INLINE_TEMPLATE_BEGIN_PATTERN.matcher(lines[i]).matches()) {
                        throw new XQLParseException("Inline template missing '//TEMPLATE-END' at: " + i);
                    }
                    if (i >= lines.length - 1) {
                        throw new XQLParseException("Inline template missing '//TEMPLATE-END' at the end");
                    }
                    sb.add(lines[i]);
                    i++;
                }
                i++;
                consumer.accept(m.group("key"), sb.toString());
            } else {
                i++;
            }
        }
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
                String template = e.getValue().getSource();
                // fix template
                if (SqlUtils.indexOfWholeLineComment(template) != -1) {
                    template = NEW_LINE + template;
                }
                int lastLN = template.lastIndexOf(NEW_LINE);
                if (lastLN != -1) {
                    String lastLine = template.substring(lastLN);
                    if (SqlUtils.indexOfWholeLineComment(lastLine) != -1) {
                        template += NEW_LINE;
                    }
                }
                templates.put(k.substring(2, k.length() - 1), template);
            }
        }
        if (templates.isEmpty() && getConstants().isEmpty()) {
            return;
        }
        for (Map.Entry<String, Sql> e : sqlResource.entrySet()) {
            Sql sql = e.getValue();
            String source = sql.getSource();
            if (source.contains("${")) {
                source = SqlUtils.formatSqlTemplate(source, templates);
                source = SqlUtils.formatSqlTemplate(source, getConstants());
                // remove empty line.
                sql.setSource(StringUtils.removeEmptyLine(source));
                log.debug("recompiling the sql '{}'", e.getKey());
            }
        }
    }

    /**
     * Read filename to FileResource object.
     *
     * @param filename filename
     * @return FileResource
     */
    protected FileResource createFileResource(@NotNull String filename) {
        return new FileResource(filename);
    }

    /**
     * Load all SQL files and parse to structured resources.
     *
     * @return structured resources
     * @throws UncheckedIOException if file not exists or read error
     * @throws RuntimeException     if uri syntax error
     */
    protected Map<String, Resource> buildResources() {
        Map<String, Resource> newResources = new LinkedHashMap<>();
        Map<String, Resource> oldResources = this.resources;
        for (Map.Entry<String, String> e : getFiles().entrySet()) {
            String alias = e.getKey();
            String filename = e.getValue();

            FileResource fr = createFileResource(filename);
            if (!fr.exists()) {
                throw new XQLParseException("XQL file '" + filename + "' of name '" + alias + "' not found!");
            }
            String ext = fr.getFilenameExtension();
            if (ext != null && (ext.equals("sql") || ext.equals("xql"))) {
                try {
                    Resource old = oldResources.get(alias);
                    if (old != null
                            && old.getFilename().equals(filename)
                            && old.getLastModified() == fr.getLastModified()) {
                        newResources.put(alias, old);
                        log.debug("Skip load unmodified resource [{}] from [{}]", alias, filename);
                    } else {
                        newResources.put(alias, parseXql(alias, filename, fr));
                    }
                } catch (URISyntaxException | IOException ex) {
                    throw new XQLParseException("Load resources failed!", ex);
                }
            }
        }
        return newResources;
    }

    /**
     * Load pipe to create instances.
     *
     * @return pipe instances
     */
    protected Map<String, IPipe<?>> buildPipeInstances() {
        final ClassLoader classLoader = FileResource.getClassLoader();
        Map<String, IPipe<?>> newPipeInstances = new HashMap<>();
        Map<String, IPipe<?>> oldPipeInstances = this.pipeInstances;
        for (Map.Entry<String, String> e : getPipes().entrySet()) {
            try {
                IPipe<?> old = oldPipeInstances.get(e.getKey());
                if (old != null) {
                    newPipeInstances.put(e.getKey(), old);
                } else {
                    IPipe<?> newPipe = (IPipe<?>) ReflectUtils.getInstance(classLoader.loadClass(e.getValue()));
                    newPipeInstances.put(e.getKey(), newPipe);
                }
            } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                     IllegalAccessException | ClassNotFoundException ex) {
                throw new XQLParseException("Create pipe instance failed.", ex);
            }
        }
        if (log.isDebugEnabled()) {
            if (!pipeInstances.isEmpty())
                log.debug("loaded pipes {}", pipeInstances);
        }
        return newPipeInstances;
    }

    /**
     * Initializes the XQLFileManager by loading resources and pipes in a thread-safe manner.
     * This method sets the loading state to true, loads all necessary resources and pipes,
     * and then sets the initialized state to true. It ensures that the initialization process
     * is atomic by using a lock.
     * <p>
     * The method should be called before any other operations on the XQLFileManager to ensure
     * that all configurations and resources are properly set up.
     */
    public void init() {
        lock.lock();
        try {
            loading = true;
            resources = Collections.unmodifiableMap(buildResources());
            pipeInstances = Collections.unmodifiableMap(buildPipeInstances());
        } finally {
            loading = false;
            initialized = true;
            lock.unlock();
        }
    }

    /**
     * Extracts the modifier from a given SQL reference string.
     * The modifier is defined as the substring following the last occurrence of the caret (^) character, e.g. {@code user.queryAll^page}
     *
     * @param sqlReference the SQL reference string to extract the modifier from
     * @return the extracted modifier, or null if no modifier is found
     */
    public static @Nullable String extractModifier(@NotNull String sqlReference) {
        int mIdx = sqlReference.lastIndexOf('^');
        if (mIdx == -1) {
            return null;
        }
        return sqlReference.substring(mIdx + 1);
    }

    /**
     * Decodes a given SQL reference string into its alias and name components.
     * The expected format of the SQL reference is {@code <alias>.<sqlName>} with an optional
     * '{@code ^}' character to indicate additional information that should be ignored.
     *
     * @param sqlReference the SQL reference string in the format {@code <alias>.<sqlName>} or {@code <alias>.<sqlName>^extra}
     * @return a Pair where the first element is the alias and the second is the sqlName
     * @throws IllegalArgumentException if the input does not follow the expected format
     */
    public static @NotNull Pair<String, String> decodeSqlReference(@NotNull String sqlReference) {
        String ref = sqlReference;
        int mIdx = ref.lastIndexOf('^');
        if (mIdx != -1) {
            ref = sqlReference.substring(0, mIdx);
        }
        int dotIdx = ref.lastIndexOf(".");
        if (dotIdx < 1) {
            throw new IllegalArgumentException("Invalid sql reference name, please follow <alias>.<sqlName> format.");
        }
        String alias = ref.substring(0, dotIdx);
        String name = ref.substring(dotIdx + 1);
        return Pair.of(alias, name);
    }

    /**
     * Encodes a SQL reference by combining an alias and a name with a dot separator.
     *
     * @param alias the alias part of the SQL reference
     * @param name  the name part of the SQL reference
     * @return the encoded SQL reference as a string in the format "alias.name"
     */
    public static @NotNull String encodeSqlReference(@NotNull String alias, @NotNull String name) {
        return alias + "." + name;
    }

    /**
     * Foreach all SQL resource.
     *
     * @param consumer (alias, resource) -&gt; void
     */
    public void foreach(BiConsumer<String, Resource> consumer) {
        resources.forEach(consumer);
    }

    /**
     * Get all SQL fragment name.
     *
     * @return SQL fragment names set
     */
    public @NotNull Set<String> names() {
        Set<String> names = new HashSet<>();
        foreach((k, r) -> r.getEntry().keySet().forEach(n -> names.add(encodeSqlReference(k, n))));
        return names;
    }

    /**
     * Get SQL fragment count.
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
     * Get sql resource.
     *
     * @param alias sql file alias
     * @return sql resource
     */
    public @Nullable Resource getResource(@NotNull String alias) {
        return resources.get(alias);
    }

    /**
     * Get all SQL resources.
     *
     * @return unmodifiable sql resources
     */
    public @NotNull @Unmodifiable Map<String, Resource> getResources() {
        return resources;
    }

    /**
     * Get all pipe instances.
     *
     * @return unmodifiable pipe instances
     */
    public @NotNull @Unmodifiable Map<String, IPipe<?>> getPipeInstances() {
        return pipeInstances;
    }

    /**
     * Check resources contains sql fragment or not.
     *
     * @param name sql reference name ({@code <alias>.<sqlName>})
     * @return true if exists or false
     */
    public boolean contains(@NotNull String name) {
        Pair<String, String> p = decodeSqlReference(name);
        Resource resource = resources.get(p.getItem1());
        if (resource == null) {
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
    public Sql getSqlObject(@NotNull String name) {
        Pair<String, String> p = decodeSqlReference(name);
        Resource resource = getResource(p.getItem1());
        if (resource == null) {
            throw new NoSuchElementException(String.format("Resource with alias [%s] not found.", p.getItem1()));
        }
        Sql sql = resource.getEntry().get(p.getItem2());
        if (sql == null) {
            throw new NoSuchElementException(String.format("No SQL named [%s] was found in [%s].", p.getItem2(), p.getItem1()));
        }
        return sql;
    }

    /**
     * Retrieves the source of the SQL object associated with the given name.
     *
     * @param name the name of the SQL object to retrieve, e.g. {@code <alias>.<sqlName>}
     * @return the source code of the SQL object as a String
     */
    public String get(@NotNull String name) {
        return getSqlObject(name).getSource();
    }

    /**
     * Retrieves and parses a dynamic SQL statement based on the provided name and arguments.
     *
     * @param name the name of the SQL statement to retrieve, e.g. {@code <alias>.<sqlName>}
     * @param args a map containing the arguments to be used in parsing the SQL statement
     * @return a Pair where the first element is the parsed SQL statement as a String, and the second element is a Map containing any additional data or parameters
     */
    public Pair<String, Map<String, Object>> get(@NotNull String name, Map<String, Object> args) {
        Sql sql = getSqlObject(name);
        ScriptAst ast = sql.getAst();
        if (ast.isDynamic()) {
            EvalContext context = new DynamicSqlEvalContext(args);
            EvalResult result = scriptEngine.execute(ast, context);
            Map<String, Object> vars = new HashMap<>(1);
            vars.put(DynamicSqlEvalContext.GENERATED_VAR_KEY, result.getUsedVars());
            return Pair.of(result.getContent(), vars);
        }
        return Pair.of(sql.getSource(), Collections.emptyMap());
    }

    /**
     * Get a constant.
     *
     * @param key constant name
     * @return constant value
     */
    public Object getConstant(String key) {
        return getConstants().get(key);
    }

    @Override
    public void setNamedParamPrefix(Character namedParamPrefix) {
        super.setNamedParamPrefix(namedParamPrefix);
        if (namedParamPrefix != null && namedParamPrefix != ' ') {
            sqlGenerator = new SqlGenerator(namedParamPrefix);
        }
    }

    /**
     * Loading state.
     *
     * @return true if loading or false
     */
    public boolean isLoading() {
        return loading;
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
     * Release runtime cache.
     */
    @Override
    public void close() {
        initialized = false;
        resources = Collections.emptyMap();
        pipeInstances = Collections.emptyMap();
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof XQLFileManager)) return false;
        if (!super.equals(o)) return false;

        XQLFileManager that = (XQLFileManager) o;
        return getResources().equals(that.getResources()) && getPipeInstances().equals(that.getPipeInstances());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getResources().hashCode();
        result = 31 * result + getPipeInstances().hashCode();
        return result;
    }

    public class DynamicSqlEvalContext extends EvalContext {
        public static final String GENERATED_VAR_KEY = "_var";
        public static final String GENERATED_VAR_PREFIX = GENERATED_VAR_KEY + ".";
        private final char namedParamPrefix = getNamedParamPrefix();
        private final Pattern namedParamPattern = sqlGenerator.getNamedParamPattern();

        public DynamicSqlEvalContext(@NotNull Map<String, Object> args) {
            super(args);
        }

        @Override
        protected @NotNull Map<String, IPipe<?>> getPipes() {
            return pipeInstances;
        }

        private String genVarName(VarMeta varMeta) {
            return varMeta.getName() + "_" + varMeta.getId();
        }

        /**
         * Format the plain text and collect the variables which used in  formatting, e.g.
         * <blockquote>
         * <pre>["CYX", "jack", "Mike"]</pre>
         * </blockquote>
         * <p>for loop:</p>
         * <blockquote>
         * <pre>
         * -- #for user of :users; last as isLast
         *    :user
         *    -- #if !:isLast
         *    ,
         *    -- #fi
         * -- #done
         * </pre>
         * </blockquote>
         * <p>result:</p>
         * <blockquote>
         * <pre>
         * :_var.user_0
         * ,
         * :_var.user_1
         * ,
         * :_var.user_2
         * </pre>
         * </blockquote>
         *
         * @param text   the plain text
         * @param inputs input arguments
         * @param scope  current scope variables
         * @return formatted content
         */
        @Override
        protected Pair<String, Map<String, Object>> formatScopePlainText(String text, Map<String, Object> inputs, Map<String, VarMeta> scope) {
            if (scope.isEmpty()) return Pair.of(text, Collections.emptyMap());
            String formatted = text;
            if (text.contains("${")) {
                Map<String, Object> scopeArgs = new HashMap<>(scope.size());
                for (Map.Entry<String, VarMeta> entry : scope.entrySet()) {
                    scopeArgs.put(entry.getKey(), entry.getValue().getValue());
                }
                formatted = SqlUtils.formatSqlTemplate(text, scopeArgs);
            }
            Map<String, Object> usedVars = new HashMap<>();
            if (formatted.indexOf(namedParamPrefix) != -1) {
                StringBuffer sb = new StringBuffer();
                Matcher m = namedParamPattern.matcher(formatted);
                while (m.find()) {
                    String name = m.group(1);
                    String replacement = null;
                    if (name != null) {
                        int idx = -1;
                        for (int i = 0; i < name.length(); i++) {
                            if (name.charAt(i) == '.') {
                                idx = i;
                                break;
                            }
                            if (name.charAt(i) == '[') {
                                idx = i;
                                break;
                            }
                        }

                        if (idx == -1 && scope.containsKey(name)) {
                            VarMeta varMeta = scope.get(name);
                            String varName = genVarName(varMeta);
                            replacement = namedParamPrefix
                                    + GENERATED_VAR_PREFIX
                                    + varName;
                            usedVars.put(varName, varMeta.getValue());
                        } else {
                            // -- #for item of :data | kv
                            //  ${item.key} = :item.value
                            //-- #done
                            // --------------------------
                            // name: item.value
                            // varName: item
                            if (idx != -1) {
                                String paramName = name.substring(0, idx);
                                if (scope.containsKey(paramName)) {
                                    VarMeta varMeta = scope.get(paramName);
                                    String varName = genVarName(varMeta);
                                    replacement = namedParamPrefix
                                            + GENERATED_VAR_PREFIX
                                            + varName
                                            + name.substring(idx);
                                    usedVars.put(varName, varMeta.getValue());
                                }
                            }
                        }
                    }
                    if (replacement != null) {
                        m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
                    } else {
                        m.appendReplacement(sb, m.group());
                    }
                }
                m.appendTail(sb);
                formatted = sb.toString();
            }
            return Pair.of(formatted, usedVars);
        }
    }

    /**
     * Sql object.
     */
    public class Sql {
        private String source;
        private ScriptAst ast;
        private Map<String, String> metadata;
        private String description = "";

        public Sql(@NotNull String source) {
            this.source = source;
            this.buildAst();
        }

        public @NotNull String getSource() {
            return source;
        }

        public @NotNull ScriptAst getAst() {
            return ast;
        }

        public @Unmodifiable Map<String, Object> getMetadata() {
            return Collections.unmodifiableMap(metadata);
        }

        public @NotNull String getDescription() {
            return description;
        }

        private void setSource(@NotNull String source) {
            this.source = source;
            this.buildAst();
        }

        void setMetadata(Map<String, String> metadata) {
            if (metadata != null) {
                this.metadata = metadata;
            }
        }

        private void setDescription(String description) {
            if (description != null)
                this.description = description;
        }

        /**
         * Compile the source SQL to ast tree.
         */
        private void buildAst() {
            this.ast = scriptEngine.compile(source);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Sql)) return false;

            Sql sql = (Sql) o;
            return getSource().equals(sql.getSource()) && getDescription().equals(sql.getDescription());
        }

        @Override
        public int hashCode() {
            int result = getSource().hashCode();
            result = 31 * result + getDescription().hashCode();
            return result;
        }
    }

    /**
     * Sql file resource.
     */
    public static final class Resource {
        private final String filename;
        private long lastModified = -1;
        private String description = "";
        private Map<String, Sql> entry;

        public Resource(@NotNull String filename) {
            this.filename = filename;
            this.entry = Collections.emptyMap();
        }

        public String getFilename() {
            return filename;
        }

        public long getLastModified() {
            return lastModified;
        }

        public @NotNull String getDescription() {
            return description;
        }

        public @NotNull Map<String, Sql> getEntry() {
            return entry;
        }

        private void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        private void setDescription(String description) {
            this.description = description;
        }

        private void setEntry(Map<String, Sql> entry) {
            if (entry != null)
                this.entry = entry;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Resource)) return false;

            Resource resource = (Resource) o;
            return getLastModified() == resource.getLastModified() && Objects.equals(getFilename(), resource.getFilename()) && Objects.equals(getDescription(), resource.getDescription()) && getEntry().equals(resource.getEntry());
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(getFilename());
            result = 31 * result + Long.hashCode(getLastModified());
            result = 31 * result + Objects.hashCode(getDescription());
            result = 31 * result + getEntry().hashCode();
            return result;
        }
    }
}
