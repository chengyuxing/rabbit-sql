package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.script.lexer.RabbitScriptLexer;
import com.github.chengyuxing.common.script.parser.RabbitScriptParser;
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

import static com.github.chengyuxing.common.util.StringUtils.NEW_LINE;
import static com.github.chengyuxing.common.util.StringUtils.containsAnyIgnoreCase;

/**
 * <h2>Dynamic SQL File Manager</h2>
 * <p>Use standard sql block comment ({@code /**}{@code /}), line comment ({@code --}),
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
 * <p>The xql object is {@code key-value} format, key is sql name, value is sql statement,
 * every xql object is delimited by single symbol {@code ;} at the end.</p>
 * <p>For some statement blocks that contain internal {@code ;} (such as PostgreSQL's {@code DO $$...$$} or PLSQL procedure body)
 * A comment marker {@code --} can be used at the end of a sentence line to prevent premature truncation, e.g.</p>
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
 * {@linkplain DynamicSqlParser Dynamic sql script} write in line comment where starts with {@code --},
 * check example following class path file: {@code home.xql.template}.
 * <p>Supported Directives: {@link com.github.chengyuxing.common.script.Directives Directives}</p>
 * <p>Invoke method {@link #get(String, Map)} to enjoy the dynamic sql!</p>
 *
 * @see RabbitScriptParser
 */
public class XQLFileManager extends XQLFileManagerConfig implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManager.class);
    public static final Pattern KEY_PATTERN = Pattern.compile("/\\*\\s*(\\[\\s*(?<sqlName>[^\\s\\[\\]{^.}]+)\\s*]|\\{\\s*(?<partName>[^\\s\\[\\]{^.}]+)\\s*})\\s*\\*/");
    public static final String XQL_DESC_QUOTE = "@@@";
    public static final String YML = "xql-file-manager.yml";

    private final ReentrantLock lock = new ReentrantLock();
    protected final Map<String, IPipe<?>> pipeInstances = new HashMap<>();
    protected volatile boolean loading;
    protected volatile boolean initialized;

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
            throw new IllegalArgumentException("Duplicate alias: " + alias);
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
     * @throws URISyntaxException if file uri syntax error
     */
    public @NotNull Resource parseXql(@NotNull String alias, @NotNull String filename, @NotNull FileResource fileResource) throws IOException, URISyntaxException {
        Map<String, Sql> entry = new LinkedHashMap<>();
        StringJoiner xqlDesc = new StringJoiner(NEW_LINE);
        try (BufferedReader reader = fileResource.getBufferedReader(Charset.forName(charset))) {
            String line;
            String currentName = null;
            boolean isMainStarted = false;
            StringBuilder sqlBuffer = new StringBuilder();
            StringBuilder descriptionBuffer = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (trimLine.isEmpty()) continue;
                Matcher matcher = KEY_PATTERN.matcher(trimLine);
                if (matcher.matches()) {
                    isMainStarted = true;
                    String sqlName = matcher.group("sqlName");
                    String partName = matcher.group("partName");
                    String name = sqlName != null ? sqlName : "${" + partName + "}";
                    if (sqlBuffer.length() != 0) {
                        throw new XQLParseException("The sql which before the name '" + ValueUtils.coalesce(sqlName, partName) + "' does not seem to end with the ';' in " + filename);
                    }
                    if (entry.containsKey(name)) {
                        throw new XQLParseException("Duplicate name: '" + ValueUtils.coalesce(sqlName, partName) + "' in " + filename);
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
                                descriptionBuffer.append(description).append(NEW_LINE);
                            }
                            continue;
                        }
                        String descriptionStart = trimLine.substring(3);
                        if (!StringUtils.isBlank(descriptionStart)) {
                            descriptionBuffer.append(descriptionStart).append(NEW_LINE);
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
                                    xqlDesc.add(tb);
                                }
                            }
                        }
                    }
                }
                if (currentName != null) {
                    sqlBuffer.append(line).append(NEW_LINE);
                    if (trimLine.endsWith(";")) {
                        String sql = sqlBuffer.toString().trim();
                        sql = sql.substring(0, sql.length() - 1).trim();
                        String desc = descriptionBuffer.toString().trim();
                        entry.put(currentName, scanSql(alias, filename, currentName, sql, desc));
                        currentName = null;
                        sqlBuffer.setLength(0);
                        descriptionBuffer.setLength(0);
                    }
                }
            }
            // if last part of sql is not ends with ';' symbol
            if (currentName != null) {
                String lastSql = sqlBuffer.toString().trim();
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
    protected @NotNull Sql scanSql(@NotNull String alias, @NotNull String filename, @NotNull String sqlName, @NotNull String sql, @NotNull String desc) {
        try {
            newDynamicSqlParser(sql).verify();
        } catch (ScriptSyntaxException e) {
            throw new XQLParseException("File: " + filename + " -> '" + sqlName + "' has script syntax error", e);
        }
        Sql sqlObj = new Sql(sql);
        sqlObj.setDescription(desc);
        log.debug("scan(;) {} to get sql [{}.{}]: {}", filename, alias, sqlName, SqlHighlighter.highlightIfAnsiCapable(sql));
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
        if (templates.isEmpty() && constants.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Sql> e : sqlResource.entrySet()) {
            Sql sql = e.getValue();
            String source = sql.getSource();
            if (source.contains("${")) {
                source = SqlUtils.formatSql(source, templates);
                source = SqlUtils.formatSql(source, constants);
                // remove empty line.
                sql.setSource(StringUtils.removeEmptyLine(source));
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
                        Resource parsed = parseXql(alias, filename, fr);
                        resource.setEntry(parsed.getEntry());
                        resource.setDescription(parsed.getDescription());
                        resource.setLastModified(parsed.getLastModified());
                    }
                } else {
                    throw new FileNotFoundException("XQL file '" + filename + "' of name '" + alias + "' not found!");
                }
            }
        } catch (Exception e) {
            throw new XQLParseException("Load resources failed!", e);
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
            final ClassLoader classLoader = FileResource.getClassLoader();
            for (Map.Entry<String, String> entry : pipes.entrySet()) {
                pipeInstances.put(entry.getKey(), (IPipe<?>) ReflectUtils.getInstance(classLoader.loadClass(entry.getValue())));
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                 InvocationTargetException | NoSuchMethodException e) {
            throw new XQLParseException("Init pipe error.", e);
        }
        if (log.isDebugEnabled()) {
            if (!pipeInstances.isEmpty())
                log.debug("loaded pipes {}", pipeInstances);
        }
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
            loadResources();
            loadPipes();
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
    public @NotNull Set<String> names() {
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
    public @Nullable Resource getResource(@NotNull String alias) {
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
    public boolean contains(@NotNull String name) {
        Pair<String, String> p = decodeSqlReference(name);
        Resource resource = files.getResources().get(p.getItem1());
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
     * @see DynamicSqlParser
     */
    public Pair<String, Map<String, Object>> get(@NotNull String name, Map<String, ?> args) {
        return parseDynamicSql(get(name), args);
    }

    /**
     * Parses the provided SQL string, replacing dynamic elements with values from the given arguments.
     * If the SQL contains any directives, it processes them and returns the modified SQL along with a map of variables defined or used in the process.
     *
     * @param sql  The SQL statement to be parsed. It may contain dynamic elements or directives that need to be processed.
     * @param args A map of argument names and their corresponding values to be used for replacing dynamic elements within the SQL. Can be null.
     * @return A Pair where the first element is the parsed (potentially modified) SQL statement, and the second element is a map containing all variables defined or utilized during the parsing process.
     */
    public Pair<String, Map<String, Object>> parseDynamicSql(@NotNull String sql, @Nullable Map<String, ?> args) {
        if (!containsAnyIgnoreCase(sql, RabbitScriptLexer.DIRECTIVES)) {
            return Pair.of(sql, Collections.emptyMap());
        }
        Map<String, Object> myArgs = new HashMap<>();
        if (args != null) {
            myArgs.putAll(args);
        }
        myArgs.put("_databaseId", databaseId);

        DynamicSqlParser parser = newDynamicSqlParser(sql);
        String parsedSql = parser.parse(myArgs);
        //noinspection ExtractMethodRecommender
        Map<String, Object> vars = new HashMap<>(parser.getDefinedVars());
        Map<String, Object> forGeneratedVars = parser.getForGeneratedVars();
        if (!forGeneratedVars.isEmpty()) {
            // #for expression temp variables stored in _for variable.
            if (!vars.containsKey(DynamicSqlParser.FOR_VARS_KEY)) {
                vars.put(DynamicSqlParser.FOR_VARS_KEY, forGeneratedVars);
            } else {
                throw new IllegalStateException("#var cannot define the name " + DynamicSqlParser.FOR_VARS_KEY + " when #for directive exists.");
            }
        }
        return Pair.of(parsedSql, vars);
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
    public class DynamicSqlParser extends RabbitScriptParser {
        public static final String FOR_VARS_KEY = "_for";
        public static final String VAR_PREFIX = FOR_VARS_KEY + '.';
        private final Pattern namedParamPattern;

        public DynamicSqlParser(@NotNull String sql) {
            super(sql);
            this.namedParamPattern = new SqlGenerator(namedParamPrefix).getNamedParamPattern();
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
         * @param forIndex  each for loop auto index
         * @param itemIndex for var auto index
         * @param body      content in for loop
         * @param context   each for loop args which created by for expression
         * @return formatted content
         */
        @Override
        protected String forLoopBodyFormatter(int forIndex, int itemIndex, @NotNull String body, @NotNull Map<String, Object> context) {
            String formatted = body;
            if (body.contains("${")) {
                formatted = SqlUtils.formatSql(body, context);
            }
            if (formatted.indexOf(namedParamPrefix) != -1) {
                StringBuffer sb = new StringBuffer();
                Matcher m = namedParamPattern.matcher(formatted);
                while (m.find()) {
                    String name = m.group(1);
                    String replacement = null;
                    if (name != null) {
                        if (context.containsKey(name)) {
                            replacement = namedParamPrefix
                                    + VAR_PREFIX
                                    + forVarGeneratedKey(name, forIndex, itemIndex);
                        } else {
                            // -- #for item of :data | kv
                            //  ${item.key} = :item.value
                            //-- #done
                            // --------------------------
                            // name: item.value
                            // varName: item
                            int dotIdx = name.indexOf('.');
                            if (dotIdx != -1) {
                                String paramName = name.substring(0, dotIdx);
                                if (context.containsKey(paramName)) {
                                    replacement = namedParamPrefix
                                            + VAR_PREFIX
                                            + forVarGeneratedKey(paramName, forIndex, itemIndex)
                                            + name.substring(dotIdx);
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
            return formatted;
        }

        /**
         * Normalizes the directive line by removing the leading '--' if present.
         *
         * @param line the line to be normalized
         * @return the normalized line with leading '--' removed, or the original line if no '--' is found
         */
        @Override
        protected String normalizeDirectiveLine(String line) {
            int idx = SqlUtils.indexOfWholeLineComment(line);
            if (idx != -1) {
                return line.substring(idx + 2);
            }
            return line;
        }
    }
}
