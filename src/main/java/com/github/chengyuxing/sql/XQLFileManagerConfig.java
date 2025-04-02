package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.io.TypedProperties;
import com.github.chengyuxing.common.script.expression.IPipe;
import com.github.chengyuxing.sql.exceptions.YamlDeserializeException;
import com.github.chengyuxing.sql.yaml.FeaturedConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;
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
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Dynamic SQL File Manager config.
 */
public class XQLFileManagerConfig {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManagerConfig.class);
    private String configLocation;
    // ----------------optional properties------------------
    protected FileMap files = new FileMap();
    protected Map<String, Object> constants = new HashMap<>();
    protected Map<String, String> pipes = new HashMap<>();
    protected String charset = "UTF-8";
    protected String delimiter = ";";
    protected Character namedParamPrefix = ':';
    protected String databaseId;
    // ----------------optional properties------------------
    protected volatile boolean loading;

    /**
     * Constructs a new XQLFileManagerConfig.
     */
    public XQLFileManagerConfig() {
    }

    /**
     * Constructs a new XQLFileManagerConfig with config location.
     *
     * @param configLocation config location path
     */
    public XQLFileManagerConfig(String configLocation) {
        this.configLocation = configLocation;
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
            FileMap localFiles = new FileMap();
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

    public String getConfigLocation() {
        return configLocation;
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
    public FileMap getFiles() {
        return files;
    }

    /**
     * Set sql files map.
     *
     * @param files files map [alias, file name]
     */
    public void setFiles(FileMap files) {
        if (Objects.nonNull(files)) {
            this.files = new FileMap(files);
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
        if (Objects.nonNull(constants)) {
            this.constants = new HashMap<>(constants);
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
        this.databaseId = databaseId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof XQLFileManagerConfig)) return false;

        XQLFileManagerConfig config = (XQLFileManagerConfig) o;

        if (!getFiles().equals(config.getFiles())) return false;
        if (!getConstants().equals(config.getConstants())) return false;
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
        result = 31 * result + getPipes().hashCode();
        result = 31 * result + getCharset().hashCode();
        result = 31 * result + getDelimiter().hashCode();
        result = 31 * result + getNamedParamPrefix().hashCode();
        result = 31 * result + (getDatabaseId() != null ? getDatabaseId().hashCode() : 0);
        return result;
    }

    /**
     * Sql object.
     */
    public static final class Sql {
        private String content;
        private String description = "";

        public Sql(@NotNull String content) {
            this.content = content;
        }

        void setContent(String content) {
            this.content = content;
        }

        public @NotNull String getContent() {
            return content;
        }

        public @NotNull String getDescription() {
            return description;
        }

        void setDescription(String description) {
            if (Objects.nonNull(description))
                this.description = description;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Sql)) return false;

            Sql sql = (Sql) o;
            return getContent().equals(sql.getContent()) && getDescription().equals(sql.getDescription());
        }

        @Override
        public int hashCode() {
            int result = getContent().hashCode();
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

        void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

        public @NotNull String getDescription() {
            return description;
        }

        void setDescription(String description) {
            this.description = description;
        }

        public @NotNull Map<String, Sql> getEntry() {
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

    /**
     * Sql file container.
     */
    public static final class FileMap extends LinkedHashMap<String, String> {
        private final Map<String, Resource> resources = new LinkedHashMap<>();

        public FileMap() {
        }

        public FileMap(@NotNull Map<String, String> files) {
            putAll(files);
        }

        public Resource getResource(String key) {
            return resources.get(key);
        }

        public @NotNull @Unmodifiable Map<String, Resource> getResources() {
            return Collections.unmodifiableMap(resources);
        }

        @Override
        public String put(String key, String value) {
            resources.put(key, new Resource(value));
            return super.put(key, value);
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> m) {
            if (m == null || m.isEmpty()) return;
            for (Map.Entry<? extends String, ? extends String> entry : m.entrySet()) {
                resources.put(entry.getKey(), new Resource(entry.getValue()));
            }
            super.putAll(m);
        }

        @Override
        public String remove(Object key) {
            resources.remove(key);
            return super.remove(key);
        }

        @Override
        public void clear() {
            resources.clear();
            super.clear();
        }

        @Override
        public String compute(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String computeIfPresent(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String merge(String key, String value, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean replace(String key, String oldValue, String newValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String replace(String key, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceAll(BiFunction<? super String, ? super String, ? extends String> function) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String putIfAbsent(String key, String value) {
            throw new UnsupportedOperationException();
        }
    }
}
