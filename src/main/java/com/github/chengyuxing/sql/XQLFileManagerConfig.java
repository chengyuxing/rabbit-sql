package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.io.TypedProperties;
import com.github.chengyuxing.common.script.expression.IPipe;
import com.github.chengyuxing.sql.exceptions.YamlDeserializeException;
import com.github.chengyuxing.sql.yaml.FeaturedConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Dynamic SQL parse file manager config.
 */
public class XQLFileManagerConfig {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManagerConfig.class);
    // ----------------optional properties------------------
    protected Map<String, String> files = new LinkedHashMap<>();
    protected Map<String, Object> constants = new HashMap<>();
    protected Map<String, IPipe<?>> pipeInstances = new HashMap<>();
    protected Map<String, String> pipes = new HashMap<>();
    protected String charset = "UTF-8";
    protected String delimiter = ";";
    protected Character namedParamPrefix = ':';
    protected String databaseId;
    // ----------------optional properties------------------
    protected volatile boolean loading;

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
        Yaml yaml = new Yaml(new FeaturedConstructor());
        try {
            XQLFileManagerConfig config = yaml.loadAs(yamlLocation.getInputStream(), XQLFileManagerConfig.class);
            if (Objects.isNull(config)) {
                log.warn("yaml loaded nothing, resource length is {}", yamlLocation.getInputStream().available());
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
            XQLFileManagerConfig config = new XQLFileManagerConfig();
            properties.load(propertiesLocation.getInputStream());
            Map<String, String> localFiles = new LinkedHashMap<>();
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

            config.setFiles(localFiles);
            config.setConstants(localConstants);
            config.setPipes(localPipes);
            config.setDelimiter(properties.getProperty("delimiter"));
            config.setCharset(properties.getProperty("charset"));
            config.setNamedParamPrefix(properties.getProperty("namedParamPrefix", ":").charAt(0));
            config.setDatabaseId(properties.getProperty("databaseId"));
            config.copyStateTo(this);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Copy config state to another.
     *
     * @param other another XQLFileManagerConfig
     */
    public void copyStateTo(XQLFileManagerConfig other) {
        Field[] fields = XQLFileManagerConfig.class.getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isFinal(field.getModifiers())) {
                field.setAccessible(true);
                try {
                    Object v = field.get(this);
                    if (Objects.nonNull(v)) {
                        field.set(other, v);
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
    void checkLoading() {
        if (loading) throw new ConcurrentModificationException("Cannot set property while loading.");
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
        checkLoading();
        if (Objects.nonNull(files)) {
            this.files = new LinkedHashMap<>(files);
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
     * <p>constants: </p>
     * <blockquote>
     * <pre>{db: "test"}</pre>
     * </blockquote>
     * <p>sql statement:</p>
     * <blockquote>
     * <pre>select ${db}.user from table</pre>
     * </blockquote>
     * <p>result: </p>
     * <blockquote>
     * <pre>select test.user from table</pre>
     * </blockquote>
     *
     * @param constants constants map
     */
    public void setConstants(Map<String, Object> constants) {
        checkLoading();
        if (Objects.nonNull(constants)) {
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
        checkLoading();
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
        checkLoading();
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
        checkLoading();
        if (Objects.nonNull(charset)) {
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
        checkLoading();
        if (Objects.nonNull(charset)) {
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
        checkLoading();
        if (Objects.nonNull(delimiter) && !delimiter.trim().isEmpty()) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof XQLFileManagerConfig)) return false;

        XQLFileManagerConfig config = (XQLFileManagerConfig) o;

        if (!getFiles().equals(config.getFiles())) return false;
        if (!getConstants().equals(config.getConstants())) return false;
        if (!getPipeInstances().equals(config.getPipeInstances())) return false;
        if (!getPipes().equals(config.getPipes())) return false;
        if (!getCharset().equals(config.getCharset())) return false;
        if (!getDelimiter().equals(config.getDelimiter())) return false;
        if (!getNamedParamPrefix().equals(config.getNamedParamPrefix())) return false;
        return getDatabaseId() != null ? getDatabaseId().equals(config.getDatabaseId()) : config.getDatabaseId() == null;
    }

    @Override
    public int hashCode() {
        int result = getFiles().hashCode();
        result = 31 * result + getConstants().hashCode();
        result = 31 * result + getPipeInstances().hashCode();
        result = 31 * result + getPipes().hashCode();
        result = 31 * result + getCharset().hashCode();
        result = 31 * result + getDelimiter().hashCode();
        result = 31 * result + getNamedParamPrefix().hashCode();
        result = 31 * result + (getDatabaseId() != null ? getDatabaseId().hashCode() : 0);
        return result;
    }
}
