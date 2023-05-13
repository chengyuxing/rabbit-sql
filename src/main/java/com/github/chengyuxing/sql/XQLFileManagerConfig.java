package com.github.chengyuxing.sql;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.common.io.TypedProperties;
import com.github.chengyuxing.common.script.IPipe;
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
import java.util.regex.Pattern;

/**
 * 支持扩展脚本解析动态SQL的文件管理器配置项
 */
public class XQLFileManagerConfig {
    private static final Logger log = LoggerFactory.getLogger(XQLFileManagerConfig.class);
    public static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    public static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");
    public static final Pattern FOR_PATTERN = Pattern.compile("(?<item>\\w+)(\\s*,\\s*(?<index>\\w+))?\\s+of\\s+:(?<list>[\\w.]+)(?<pipes>(\\s*\\|\\s*[\\w.]+)*)?(\\s+delimiter\\s+'(?<delimiter>[^']*)')?(\\s+filter\\s+(?<filter>[\\S\\s]+))?");
    public static final Pattern SWITCH_PATTERN = Pattern.compile(":(?<name>[\\w.]+)\\s*(?<pipes>(\\s*\\|\\s*\\w+)*)?");
    public static final String PROPERTIES = "xql-file-manager.properties";
    public static final String YML = "xql-file-manager.yml";

    // ----------------dynamic sql expression tag------------------
    public static final String IF = "#if";
    public static final String FI = "#fi";
    public static final String CHOOSE = "#choose";
    public static final String WHEN = "#when";
    public static final String SWITCH = "#switch";
    public static final String CASE = "#case";
    public static final String FOR = "#for";
    public static final String DEFAULT = "#default";
    public static final String BREAK = "#break";
    public static final String END = "#end";
    // ----------------dynamic sql expression tag------------------

    private SqlTranslator sqlTranslator;
    protected volatile boolean loading;

    // ----------------optional properties------------------
    protected Map<String, String> files = new HashMap<>();
    protected Set<String> filenames = new HashSet<>();
    protected Map<String, String> constants = new HashMap<>();
    protected Map<String, IPipe<?>> pipeInstances = new HashMap<>();
    protected Map<String, String> pipes = new HashMap<>();
    protected int checkPeriod = 30; //seconds
    protected volatile boolean checkModified;
    protected String charset = "UTF-8";
    protected String delimiter = ";";
    protected char namedParamPrefix = ':';
    protected boolean highlightSql = false;
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
        XQLFileManagerConfig config = yaml.loadAs(yamlLocation.getInputStream(), XQLFileManagerConfig.class);
        config.copyStateTo(this);
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
            setFilenames(properties.getSet("filenames", new HashSet<>()));
            setDelimiter(properties.getProperty("delimiter", ";"));
            setCharset(properties.getProperty("charset", "UTF-8"));
            setNamedParamPrefix(properties.getProperty("namedParamPrefix", ":").charAt(0));
            setHighlightSql(properties.getBool("highlightSql", false));
            setCheckPeriod(properties.getInt("checkPeriod", 30));
            setCheckModified(properties.getBool("checkModified", false));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void copyStateTo(XQLFileManagerConfig other) {
        Field[] fields = XQLFileManagerConfig.class.getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isFinal(field.getModifiers())) {
                field.setAccessible(true);
                try {
                    field.set(other, field.get(this));
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
     * 设置命名的sql文件
     *
     * @param files 文件 [别名，文件名]
     */
    public void setFiles(Map<String, String> files) {
        this.files = files;
    }

    /**
     * 设置文件全路径名，默认别名为文件名（不包含后缀）
     *
     * @param filenames 文件全路径名
     */
    public void setFilenames(Set<String> filenames) {
        this.filenames = filenames;
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
        checkLoading();
        this.constants = constants;
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
     * @see IPipe
     */
    public void setPipeInstances(Map<String, IPipe<?>> pipeInstances) {
        this.pipeInstances = pipeInstances;
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
        this.pipes = pipes;
    }

    /**
     * 获取文件检查周期，默认为30秒，配合方法：{@link #setCheckModified(boolean)} {@code -> true} 来使用
     *
     * @return 文件检查周期
     */
    public int getCheckPeriod() {
        return checkPeriod;
    }

    /**
     * 设置文件检查周期（单位：秒）
     *
     * @param checkPeriod 文件检查周期，默认30秒
     */
    public void setCheckPeriod(int checkPeriod) {
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
     */
    public boolean isCheckModified() {
        return checkModified;
    }

    /**
     * 设置检查文件是否更新
     *
     * @param checkModified 是否检查更新
     */
    public void setCheckModified(boolean checkModified) {
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
     *     <li>也可以设置为null或空白，那么整个SQL文件多段SQL都应按照此方式分隔。</li>
     * </ul>
     *
     * @param delimiter sql块分隔符
     */
    public void setDelimiter(String delimiter) {
        checkLoading();
        this.delimiter = delimiter;
    }

    /**
     * 获取命名参数前缀，主要针对sql中形如：{@code ${:name}} 这样的情况
     *
     * @return 命名参数前缀
     */
    public char getNamedParamPrefix() {
        return namedParamPrefix;
    }

    /**
     * 设置命名参数前缀，主要针对sql中形如：{@code ${:name}} 这样的情况
     *
     * @param namedParamPrefix 命名参数前缀
     */
    public void setNamedParamPrefix(char namedParamPrefix) {
        this.namedParamPrefix = namedParamPrefix;
        this.sqlTranslator = new SqlTranslator(namedParamPrefix);
    }

    /**
     * debug模式下终端标准输出sql语法是否高亮
     *
     * @return 是否高亮
     */
    public boolean isHighlightSql() {
        return highlightSql;
    }

    /**
     * 设置debug模式下终端标准输出sql语法是否高亮
     *
     * @param highlightSql 是否高亮
     */
    public void setHighlightSql(boolean highlightSql) {
        this.highlightSql = highlightSql;
    }
}
