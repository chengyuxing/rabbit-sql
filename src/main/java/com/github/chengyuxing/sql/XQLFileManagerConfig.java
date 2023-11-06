package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.io.TypedProperties;
import com.github.chengyuxing.common.script.IPipe;
import com.github.chengyuxing.sql.exceptions.YamlDeserializeException;
import com.github.chengyuxing.sql.yaml.JoinConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Dynamic SQL parse file manager config.
 */
public class XQLFileManagerConfig {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManagerConfig.class);
    protected volatile boolean loading;

    // ----------------optional properties------------------
    protected Map<String, String> files = new HashMap<>();
    protected Map<String, Object> constants = new HashMap<>();
    protected Map<String, IPipe<?>> pipeInstances = new HashMap<>();
    protected Map<String, String> pipes = new HashMap<>();
    protected String charset = "UTF-8";
    protected String delimiter = ";";
    protected Character namedParamPrefix = ':';
    protected String databaseId;
    // ----------------optional properties------------------

    /**
     * 配置项构造器
     */
    public XQLFileManagerConfig() {
    }

    /**
     * Constructs a XQLFileManagerConfig with config location.
     *
     * @param configLocation config location path
     */
    public XQLFileManagerConfig(String configLocation) {
        this();
        FileResource resource = new FileResource(configLocation);
        if (configLocation.endsWith(".yml")) {
            loadYaml(resource);
            return;
        }
        if (configLocation.endsWith(".properties")) {
            loadProperties(resource);
            return;
        }
        throw new IllegalArgumentException("Not support file type: " + configLocation);
    }

    /**
     * Load yml config.
     *
     * @param yamlLocation yml resource
     */
    public void loadYaml(FileResource yamlLocation) {
        Yaml yaml = new Yaml(new JoinConstructor());
        try {
            XQLFileManagerConfig config = yaml.loadAs(yamlLocation.getInputStream(), XQLFileManagerConfig.class);
            if (Objects.isNull(config)) {
                log.warn("yaml loaded nothing, resource length is " + yamlLocation.getInputStream().available());
                return;
            }
            config.copyStateTo(this);
        } catch (Exception e) {
            throw new YamlDeserializeException("load yaml config error.", e);
        }
    }

    /**
     * Load properties config.
     *
     * @param propertiesLocation properties resource
     */
    public void loadProperties(FileResource propertiesLocation) {
        TypedProperties properties = new TypedProperties();
        try {
            properties.load(propertiesLocation.getInputStream());
            Map<String, String> localFiles = new HashMap<>();
            Map<String, Object> localConstants = new HashMap<>();
            Map<String, String> localPipes = new HashMap<>();
            properties.forEach((k, s) -> {
                String p = k.toString().trim();
                String v = s.toString().trim();
                if (!p.isEmpty() && !v.isEmpty()) {
                    if (p.startsWith("files.")) {
                        localFiles.put(p.substring(6), v);
                    } else if (p.startsWith("constants.")) {
                        localConstants.put(p.substring(10), v);
                    } else if (p.startsWith("pipes.")) {
                        localPipes.put(p.substring(6), v);
                    }
                }
            });

            setFiles(localFiles);
            setConstants(localConstants);
            setPipes(localPipes);
            setDelimiter(properties.getProperty("delimiter"));
            setCharset(properties.getProperty("charset"));
            setNamedParamPrefix(properties.getProperty("namedParamPrefix", ":").charAt(0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Copy config state to another.
     *
     * @param another another XQLFileManagerConfig
     */
    public void copyStateTo(XQLFileManagerConfig another) {
        Field[] fields = XQLFileManagerConfig.class.getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isFinal(field.getModifiers())) {
                field.setAccessible(true);
                try {
                    Object v = field.get(this);
                    if (Objects.nonNull(v)) {
                        field.set(another, v);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to copy XQLFileManagerConfig state: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * If XQL file manager on loading while config state changing by another thread, throws exception.
     */
    protected void checkLoading() {
        if (loading) {
            throw new ConcurrentModificationException("Cannot set property while loading.");
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
     * Get sql files map.
     *
     * @return file map [alias, file name]
     */
    public Map<String, String> getFiles() {
        return files;
    }

    /**
     * Set sql files map.
     *
     * @param files files map [alias, file name]
     */
    public void setFiles(Map<String, String> files) {
        if (Objects.nonNull(files)) {
            this.files = new HashMap<>(files);
        }
    }

    /**
     * Get constants map.
     *
     * @return constants map
     */
    public Map<String, Object> getConstants() {
        return constants;
    }

    /**
     * Set constants map.<br>
     * Example：
     * <blockquote>
     * <pre>constants: {db: "test"}</pre>
     * <pre>sql: {@code select ${db}.user from table;}</pre>
     * <pre>result: select test.user from table.</pre>
     * </blockquote>
     *
     * @param constants constants map
     */
    public void setConstants(Map<String, Object> constants) {
        if (Objects.nonNull(constants)) {
            checkLoading();
            this.constants = new HashMap<>(constants);
        }
    }

    /**
     * Get custom pipe instances map.
     *
     * @return custom pipe instances map
     */
    public Map<String, IPipe<?>> getPipeInstances() {
        return pipeInstances;
    }

    /**
     * Set custom pipe instances map.
     *
     * @param pipeInstances custom pipe instances map [pipe name, pipe instance]
     */
    public void setPipeInstances(Map<String, IPipe<?>> pipeInstances) {
        if (Objects.nonNull(pipeInstances)) {
            this.pipeInstances = new HashMap<>(pipeInstances);
        }
    }

    /**
     * Get custom pipe class name map.
     *
     * @return custom pipe class name map
     */
    public Map<String, String> getPipes() {
        return pipes;
    }

    /**
     * Set custom pipe class name map.
     *
     * @param pipes custom pipe class name map [pipe name, pipe class name]
     * @see IPipe
     */
    public void setPipes(Map<String, String> pipes) {
        if (Objects.nonNull(pipes)) {
            this.pipes = new HashMap<>(pipes);
        }
    }

    /**
     * Get sql file parsing charset.
     *
     * @return charset name
     */
    public String getCharset() {
        return charset;
    }

    /**
     * Set sql file parsing charset, UTF-8 is default.
     *
     * @param charset charset
     * @see StandardCharsets
     */
    public void setCharset(String charset) {
        if (Objects.nonNull(charset)) {
            this.checkLoading();
            this.charset = charset;
        }
    }

    /**
     * Set sql file parsing charset, UTF-8 is default.
     *
     * @param charset charset
     * @see StandardCharsets
     */
    public void setCharset(Charset charset) {
        if (Objects.nonNull(charset)) {
            checkLoading();
            this.charset = charset.name();
        }
    }

    /**
     * Get delimiter of multi sql fragment/template, symbol ({@code ;}) is default.
     *
     * @return delimiter
     */
    public String getDelimiter() {
        return delimiter;
    }

    /**
     * Set delimiter of multi sql fragment/template, symbol ({@code ;}) is default.<br>
     * Sometimes default delimiter is not enough, such as one procedure body or plsql maybe contains
     * more than one sql statement which ends with  {@code ;}, for correct set to other is necessary, like {@code ;;} .
     *
     * @param delimiter multi sql fragment/template delimiter
     */
    public void setDelimiter(String delimiter) {
        if (Objects.nonNull(delimiter) && !delimiter.trim().isEmpty()) {
            checkLoading();
            this.delimiter = delimiter;
        }
    }

    /**
     * Get named parameter prefix.
     *
     * @return named parameter prefix
     */
    public Character getNamedParamPrefix() {
        return namedParamPrefix;
    }

    /**
     * Set named parameter prefix. (for IDEA Rabbit-SQL plugin)
     *
     * @param namedParamPrefix named parameter prefix
     */
    public void setNamedParamPrefix(Character namedParamPrefix) {
        if (Objects.nonNull(namedParamPrefix) && namedParamPrefix != ' ') {
            this.namedParamPrefix = namedParamPrefix;
        }
    }

    /**
     * Get database name.
     *
     * @return database name
     */
    public String getDatabaseId() {
        return databaseId;
    }

    /**
     * Set database name.
     *
     * @param databaseId database name
     */
    public void setDatabaseId(String databaseId) {
        checkLoading();
        this.databaseId = databaseId;
    }
}
