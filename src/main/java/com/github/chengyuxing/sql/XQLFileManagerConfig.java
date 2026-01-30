package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.io.TypedProperties;
import com.github.chengyuxing.common.script.pipe.IPipe;
import com.github.chengyuxing.sql.exceptions.XQLParseException;
import com.github.chengyuxing.sql.yaml.FeaturedConstructor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * XQL File Manager config.
 */
public class XQLFileManagerConfig {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManagerConfig.class);
    private String configLocation;
    // ----------------optional properties------------------
    private Map<String, String> files = new LinkedHashMap<>();
    private Map<String, String> pipes = new HashMap<>();
    private Map<String, Object> constants = new HashMap<>();
    private String charset = "UTF-8";
    private Character namedParamPrefix = ':';
    // ----------------optional properties------------------

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
            if (config == null) {
                log.warn("yaml loaded nothing, resource length is {}", yamlLocation.getInputStream().available());
                return;
            }
            config.copyStateTo(this);
        } catch (Exception e) {
            throw new XQLParseException("Load yaml config '" + yamlLocation.getFileName() + "' failed.", e);
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
            config.setCharset(properties.getProperty("charset"));
            config.setNamedParamPrefix(properties.getProperty("namedParamPrefix", ":").charAt(0));
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
        other.setConstants(this.constants);
        other.setPipes(this.pipes);
        other.setCharset(this.charset);
        other.setNamedParamPrefix(this.namedParamPrefix);
        other.setFiles(this.files);
    }

    public String getConfigLocation() {
        return configLocation;
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
        if (files != null) {
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
        if (constants != null) {
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
        if (pipes != null) {
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
        if (charset != null) {
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
        if (charset != null) {
            this.charset = charset.name();
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
     * Set named parameter prefix.
     *
     * @param namedParamPrefix named parameter prefix
     */
    public void setNamedParamPrefix(Character namedParamPrefix) {
        if (namedParamPrefix != null && namedParamPrefix != ' ') {
            this.namedParamPrefix = namedParamPrefix;
        }
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
        return getNamedParamPrefix().equals(config.getNamedParamPrefix());
    }

    @Override
    public int hashCode() {
        int result = getFiles().hashCode();
        result = 31 * result + getConstants().hashCode();
        result = 31 * result + getPipes().hashCode();
        result = 31 * result + getCharset().hashCode();
        result = 31 * result + getNamedParamPrefix().hashCode();
        return result;
    }

    /**
     * Sql object.
     */
    public static final class Sql {
        private String source;
        private String description = "";

        public Sql(@NotNull String source) {
            this.source = source;
        }

        void setSource(@NotNull String source) {
            this.source = source;
        }

        public @NotNull String getSource() {
            return source;
        }

        public @NotNull String getDescription() {
            return description;
        }

        void setDescription(String description) {
            if (description != null)
                this.description = description;
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
