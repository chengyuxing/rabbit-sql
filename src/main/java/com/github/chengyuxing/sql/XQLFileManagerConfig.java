package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.io.TypedProperties;
import com.github.chengyuxing.common.script.IPipe;
import com.github.chengyuxing.sql.exceptions.YamlDeserializeException;
import com.github.chengyuxing.sql.utils.SqlTranslator;
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
import java.util.*;

/**
 * 支持扩展脚本解析动态SQL的文件管理器配置项
 */
public class XQLFileManagerConfig {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManagerConfig.class);
    private SqlTranslator sqlTranslator;
    protected volatile boolean loading;

    // ----------------optional properties------------------
    protected Map<String, String> files = new HashMap<>();
    @Deprecated
    protected Set<String> filenames = new HashSet<>();
    protected Map<String, String> constants = new HashMap<>();
    protected Map<String, IPipe<?>> pipeInstances = new HashMap<>();
    protected Map<String, String> pipes = new HashMap<>();
    @Deprecated
    protected Integer checkPeriod = 30; //seconds
    @Deprecated
    protected volatile Boolean checkModified = false;
    protected String charset = "UTF-8";
    protected String delimiter = ";";
    protected Character namedParamPrefix = ':';
    @Deprecated
    protected Boolean highlightSql = false;
    // ----------------optional properties------------------

    /**
     * 配置项构造器
     */
    public XQLFileManagerConfig() {
        this.sqlTranslator = new SqlTranslator(namedParamPrefix);
    }

    /**
     * 配置项构造器
     *
     * @param configLocation 配置文件路径名
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
     * 加载yml配置文件初始化配置项
     *
     * @param yamlLocation yml文件资源
     */
    public void loadYaml(FileResource yamlLocation) {
        Yaml yaml = new Yaml(new JoinConstructor());
        try {
            XQLFileManagerConfig config = yaml.loadAs(yamlLocation.getInputStream(), XQLFileManagerConfig.class);
            if (config == null) {
                log.warn("yaml loaded nothing, resource length is " + yamlLocation.getInputStream().available());
                return;
            }
            config.copyStateTo(this);
        } catch (Exception e) {
            throw new YamlDeserializeException("load yaml config error: ", e);
        }
    }

    /**
     * 加载properties配置文件初始化配置项
     *
     * @param propertiesLocation properties文件资源
     */
    public void loadProperties(FileResource propertiesLocation) {
        TypedProperties properties = new TypedProperties();
        try {
            properties.load(propertiesLocation.getInputStream());
            Map<String, String> localFiles = new HashMap<>();
            Map<String, String> localConstants = new HashMap<>();
            Map<String, String> localPipes = new HashMap<>();
            properties.forEach((k, s) -> {
                String p = k.toString().trim();
                String v = s.toString().trim();
                if (!p.equals("") && !v.equals("")) {
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
     * 拷贝所有配置项到另一个配置对象
     *
     * @param other 另一个配置对象
     */
    public void copyStateTo(XQLFileManagerConfig other) {
        Field[] fields = XQLFileManagerConfig.class.getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isFinal(field.getModifiers())) {
                field.setAccessible(true);
                try {
                    Object v = field.get(this);
                    if (v != null) {
                        field.set(other, v);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to copy XQLFileManagerConfig state: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 如果在多线程情况下进行非法操作，抛出异常
     */
    protected void checkLoading() {
        if (loading) {
            throw new ConcurrentModificationException("Cannot set property while loading.");
        }
    }

    /**
     * 获取sql翻译解析器
     *
     * @return sql翻译解析器
     */
    public SqlTranslator getSqlTranslator() {
        return sqlTranslator;
    }

    /**
     * 检查是否正在加载sql文件
     *
     * @return 是否正在加载sql文件
     */
    public boolean isLoading() {
        return loading;
    }

    /**
     * 命名的sql文件
     *
     * @return 文件字典 [别名，文件名]
     */
    public Map<String, String> getFiles() {
        return files;
    }

    /**
     * 设置命名的sql文件
     *
     * @param files 文件字典 [别名，文件名]
     */
    public void setFiles(Map<String, String> files) {
        if (files == null) {
            return;
        }
        this.files = new HashMap<>(files);
    }

    /**
     * 获取常量集合
     *
     * @return 常量集合
     */
    public Map<String, String> getConstants() {
        return constants;
    }

    /**
     * 设置全局常量集合<br>
     * 初始化扫描sql时，如果sql文件中没有找到匹配的字符串模版，则从全局常量中寻找
     * 格式为：
     * <blockquote>
     * <pre>constants: {db:"test"}</pre>
     * <pre>sql: {@code select ${db}.user from table;}</pre>
     * <pre>result: select test.user from table.</pre>
     * </blockquote>
     *
     * @param constants 常量集合
     */
    public void setConstants(Map<String, String> constants) {
        if (constants == null) {
            return;
        }
        checkLoading();
        this.constants = new HashMap<>(constants);
    }

    /**
     * 获取动态sql脚本自定义管道字典
     *
     * @return 自定义管道字典
     */
    public Map<String, IPipe<?>> getPipeInstances() {
        return pipeInstances;
    }

    /**
     * 配置动态sql脚本自定义管道字典
     *
     * @param pipeInstances 自定义管道字典 [管道名, 管道类实例]
     */
    public void setPipeInstances(Map<String, IPipe<?>> pipeInstances) {
        if (pipeInstances == null) {
            return;
        }
        this.pipeInstances = new HashMap<>(pipeInstances);
    }

    /**
     * 获取动态sql脚本自定义管道字典
     *
     * @return 自定义管道字典
     */
    public Map<String, String> getPipes() {
        return pipes;
    }

    /**
     * 配置动态sql脚本自定义管道字典
     *
     * @param pipes 自定义管道字典 [管道名, 管道类全名]
     * @see IPipe
     */
    public void setPipes(Map<String, String> pipes) {
        if (pipes == null) {
            return;
        }
        this.pipes = new HashMap<>(pipes);
    }

    /**
     * 获取文件检查周期，默认为30秒，配合方法：{@link #setCheckModified(Boolean)} {@code -> true} 来使用
     *
     * @return 文件检查周期
     * @deprecated no supported
     */
    @Deprecated
    public Integer getCheckPeriod() {
        return checkPeriod;
    }

    /**
     * 设置文件检查周期（单位：秒）
     *
     * @param checkPeriod 文件检查周期，默认30秒
     * @deprecated no supported
     */
    @Deprecated
    public void setCheckPeriod(Integer checkPeriod) {
        if (checkPeriod == null) {
            return;
        }
        if (checkPeriod < 5) {
            this.checkPeriod = 5;
            log.warn("period cannot less than 5 seconds, auto set 5 seconds.");
        } else
            this.checkPeriod = checkPeriod;
    }

    /**
     * 是否启用文件自动检查更新
     *
     * @return 文件检查更新状态
     * @deprecated no supported
     */
    @Deprecated
    public Boolean isCheckModified() {
        return checkModified;
    }

    /**
     * 设置检查文件是否更新
     *
     * @param checkModified 是否检查更新
     * @deprecated no supported
     */
    @Deprecated
    public void setCheckModified(Boolean checkModified) {
        if (checkModified == null) {
            return;
        }
        this.checkModified = checkModified;
    }

    /**
     * 获取当前解析sql文件使用的编码格式，默认为UTF-8
     *
     * @return 当前解析sql文件使用的编码格式
     */
    public String getCharset() {
        return charset;
    }

    /**
     * 设置解析sql文件使用的编码格式，默认为UTF-8
     *
     * @param charset 编码
     * @see StandardCharsets
     */
    public void setCharset(String charset) {
        if (charset == null) {
            return;
        }
        this.checkLoading();
        this.charset = charset;
    }

    /**
     * 设置解析sql文件使用的编码格式，默认为UTF-8
     *
     * @param charset 编码
     * @see StandardCharsets
     */
    public void setCharset(Charset charset) {
        if (charset == null) {
            return;
        }
        checkLoading();
        this.charset = charset.name();
    }

    /**
     * 获取当前的每个文件的sql片段块解析分隔符，默认是单个分号（{@code ;}）
     *
     * @return sql块分隔符
     */
    public String getDelimiter() {
        return delimiter;
    }

    /**
     * 每个文件的sql片段块解析分隔符，每一段完整的sql根据此设置来进行区分，
     * 默认是单个分号（{@code ;}）遵循标准sql文件多段sql分隔符。<br>但是有一种情况，如果sql文件内有<b>psql</b>：{@code create function...} 或 {@code create procedure...}等，
     * 内部会包含多段sql多个分号，为防止解析异常，单独设置自定义的分隔符：
     * <ul>
     *     <li>例如（{@code ;;}）双分号，也是标准sql所支持的, <b>并且支持仅扫描已命名的sql</b>；</li>
     * </ul>
     *
     * @param delimiter sql块分隔符
     */
    public void setDelimiter(String delimiter) {
        if (delimiter == null || delimiter.trim().equals("")) {
            return;
        }
        checkLoading();
        this.delimiter = delimiter;
    }

    /**
     * 获取命名参数前缀，主要针对sql中形如：{@code ${:name}} 这样的情况
     *
     * @return 命名参数前缀
     */
    public Character getNamedParamPrefix() {
        return namedParamPrefix;
    }

    /**
     * 设置命名参数前缀，主要针对sql中形如：{@code ${:name}} 这样的情况
     *
     * @param namedParamPrefix 命名参数前缀
     */
    public void setNamedParamPrefix(Character namedParamPrefix) {
        if (namedParamPrefix == null || namedParamPrefix == ' ') {
            return;
        }
        this.namedParamPrefix = namedParamPrefix;
        this.sqlTranslator = new SqlTranslator(this.namedParamPrefix);
    }

    /**
     * debug模式下终端标准输出sql语法是否高亮
     *
     * @return 是否高亮
     */
    @Deprecated
    public Boolean isHighlightSql() {
        return highlightSql;
    }

    /**
     * 设置debug模式下终端标准输出sql语法是否高亮
     *
     * @param highlightSql 是否高亮
     */
    @Deprecated
    public void setHighlightSql(Boolean highlightSql) {
        if (highlightSql == null) {
            return;
        }
        this.highlightSql = highlightSql;
    }

    /**
     * 获取文件名集合
     *
     * @return 文件名集合
     * @see #getFiles()
     * @deprecated 已弃用
     */
    @Deprecated
    public Set<String> getFilenames() {
        return filenames;
    }

    /**
     * 设置文件名集合
     *
     * @param filenames 文件名集合
     * @see #setFiles(Map)
     * @deprecated 已弃用
     */
    @Deprecated
    public void setFilenames(Set<String> filenames) {
        this.filenames = filenames;
    }
}
