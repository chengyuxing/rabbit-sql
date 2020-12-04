package rabbit.sql.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.common.io.FileResource;
import rabbit.common.utils.ResourceUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL文件解析管理器<br>
 * 本地sql文件 (以file:...开头)，默认读取classpath下的sql文件<br>
 * 格式参考data.sql.template
 */
public final class SQLFileManager {
    private final static Logger log = LoggerFactory.getLogger(SQLFileManager.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, String> RESOURCE = new HashMap<>();
    private Map<String, String> namedPaths;
    private List<String> unnamedPaths;
    private final AtomicInteger UN_NAMED_PATH_INDEX = new AtomicInteger();
    private final String UN_NAMED_PATH_NAME = "UN_NAMED_SQL_";
    private final Map<String, Long> LAST_MODIFIED = new HashMap<>();
    private String[] paths;
    private volatile boolean checkModified;

    private static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    private static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");

    public SQLFileManager() {
    }

    /**
     * Sql文件解析器实例<br>
     * 文件名必须从classpath目录开始到文件名(包含后缀.sql)，多个sql文件以逗号(,)分割
     *
     * @param paths 多个文件名
     */
    public SQLFileManager(String paths) {
        this.paths = paths.split(",");
    }

    /**
     * Sql文件解析器实例，文件名必须从classpath目录开始到文件名(包含后缀.sql)
     *
     * @param path  sql文件名
     * @param paths 更多sql文件名
     */
    public SQLFileManager(String path, String... paths) {
        String[] pathArr = new String[1 + paths.length];
        pathArr[0] = path;
        System.arraycopy(paths, 0, pathArr, 1, paths.length);
        this.paths = pathArr;
    }

    /**
     * 添加命名的sql文件
     *
     * @param name 名称
     * @param path sql文件全路径
     */
    public void addNamedPath(String name, String path) {
        if (namedPaths == null) {
            namedPaths = new HashMap<>();
        }
        namedPaths.put(name, path);
    }

    /**
     * 设置命名的sql文件
     *
     * @param namedPaths 命名sql文件名和路径对应关系
     */
    public void setNamedPaths(Map<String, String> namedPaths) {
        this.namedPaths = namedPaths;
    }

    /**
     * 添加未命名的sql文件
     *
     * @param path sql文件全路径
     */
    public void addUnnamedPath(String path) {
        if (path == null) {
            unnamedPaths = new ArrayList<>();
        }
        unnamedPaths.add(path);
    }

    /**
     * 设置未命名的sql文件
     *
     * @param unNamedPaths sql文件全路径列表
     */
    public void setUnnamedPaths(List<String> unNamedPaths) {
        this.unnamedPaths = unNamedPaths;
    }

    /**
     * 解析sql文件
     *
     * @param resource 类路径sql资源
     * @throws IOException IOexp
     */
    private void resolveSqlContent(String name, FileResource resource) throws IOException {
        Map<String, String> singleResource = new HashMap<>();
        String pkgPath = ResourceUtil.path2package(resource.getPath());
        String prefix = pkgPath.substring(0, pkgPath.length() - 3);
        if (!name.startsWith(UN_NAMED_PATH_NAME)) {
            prefix = name + ".";
        }
        String previousSqlName = "";
        boolean isAnnotation = false;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (!trimLine.isEmpty()) {
                    Matcher m_name = NAME_PATTERN.matcher(trimLine);
                    Matcher m_part = PART_PATTERN.matcher(trimLine);
                    if (m_name.matches()) {
                        previousSqlName = prefix + m_name.group("name");
                        if (singleResource.containsKey(previousSqlName) || RESOURCE.containsKey(previousSqlName)) {
                            throw new RuntimeException("same sql name: " + previousSqlName);
                        }
                        singleResource.put(previousSqlName, "");
                    } else if (m_part.matches()) {
                        previousSqlName = "${" + prefix + m_part.group("part") + "}";
                        if (singleResource.containsKey(previousSqlName)) {
                            throw new RuntimeException("same sql part: " + previousSqlName);
                        }
                        singleResource.put(previousSqlName, "");
                    } else {
                        // 排除单行注释
                        // --#if和--#fi此类扩展注释作为表达式做逻辑判断，不过滤
                        if (!trimLine.startsWith("--") || trimLine.startsWith("--#if") || trimLine.startsWith("--#fi")) {
                            // 排除块注释
                            if (trimLine.startsWith("/*")) {
                                // 没有找到块注释结束 则标记下面的代码都是注释
                                if (!trimLine.endsWith("*/")) {
                                    isAnnotation = true;
                                    // 如果注释结束在单行sql中间，截取注释后的sql，并标记注释结束
                                    if (trimLine.contains("*/")) {
                                        String partOfLine = singleResource.get(previousSqlName) + line.substring(line.indexOf("*/") + 2);
                                        singleResource.put(previousSqlName, partOfLine.concat("\n"));
                                        isAnnotation = false;
                                    }
                                }
                                // 如果是在注释块内
                            } else if (isAnnotation) {
                                // 如果注释此行结尾是注释，则标记为注释结束
                                if (trimLine.endsWith("*/")) {
                                    isAnnotation = false;
                                    // 如果注释结束在此行的中间，截取注释后的sql，并标记注释结束
                                } else if (trimLine.contains("*/")) {
                                    String partOfLine = singleResource.get(previousSqlName) + line.substring(line.indexOf("*/") + 2);
                                    singleResource.put(previousSqlName, partOfLine.concat("\n"));
                                    isAnnotation = false;
                                }
                            } else if (!previousSqlName.equals("")) {
                                String prepareLine = singleResource.get(previousSqlName) + line;
                                if (trimLine.endsWith(";")) {
                                    singleResource.put(previousSqlName, prepareLine.substring(0, prepareLine.lastIndexOf(";")));
                                    log.debug("scan to get SQL [{}]：{}", previousSqlName, singleResource.get(previousSqlName));
                                    previousSqlName = "";
                                } else {
                                    singleResource.put(previousSqlName, prepareLine.concat("\n"));
                                }
                            }
                        }
                    }
                }
            }
        }
        mergeSqlPartIfNecessary(singleResource, prefix);
        RESOURCE.putAll(singleResource);
    }

    /**
     * 执行合并SQL片段
     *
     * @param partName sql片段名
     */
    private void doMergeSqlPart(final String partName, final String prefix, Map<String, String> sqlResource) {
        // 此处排除sql的包名和sql名，sql片段内部合并以sql片段的名字为准
        String pn = "${" + partName.substring(prefix.length() + 2);
        boolean has = false;
        // 片段内也可以包含片段
        for (String key : sqlResource.keySet()) {
            if (sqlResource.get(key).contains(pn)) {
                sqlResource.put(key, sqlResource.get(key).replace(pn, sqlResource.get(partName)));
            }
            //直到sql内不再包含片段为止
            if (sqlResource.get(key).contains(pn)) {
                has = true;
            }
        }
        if (has) {
            doMergeSqlPart(partName, prefix, sqlResource);
        }
    }

    /**
     * 合并SQL可复用片段到包含片段名的SQL中
     */
    private void mergeSqlPartIfNecessary(Map<String, String> sqlResource, String prefix) {
        sqlResource.keySet().stream()
                .filter(k -> k.startsWith("${"))
                .forEach(k -> doMergeSqlPart(k, prefix, sqlResource));
    }

    /**
     * 获取没有命名的防止重复的自增sql文件路径名
     *
     * @return sql路径名
     */
    private String getUnnamedPathName() {
        return UN_NAMED_PATH_NAME + UN_NAMED_PATH_INDEX.getAndIncrement();
    }

    /**
     * 整合所有命名和无命名Sql
     *
     * @return 所有sql
     */
    private Map<String, String> allPaths() {
        AtomicInteger index = new AtomicInteger(0);
        Map<String, String> pathMap = new HashMap<>();
        // add unnamed paths
        if (unnamedPaths != null && unnamedPaths.size() > 0) {
            for (String path : unnamedPaths) {
                pathMap.put(getUnnamedPathName(), path);
            }
        }
        // add paths from constructor's args
        if (paths != null && paths.length > 0) {
            for (String path : paths) {
                pathMap.put(getUnnamedPathName(), path);
            }
        }
        // add named paths
        if (namedPaths != null && namedPaths.size() > 0) {
            pathMap.putAll(namedPaths);
        }
        return pathMap;
    }

    /**
     * 如果有检测到文件修改过，则重新加载已修改过的sql文件
     */
    private void reloadIfNecessary() throws IOException, URISyntaxException {
        lock.lock();
        try {
            if (!LAST_MODIFIED.isEmpty()) {
                Map<String, String> mappedPaths = allPaths();
                for (String name : mappedPaths.keySet()) {
                    FileResource cr = new FileResource(mappedPaths.get(name));
                    if (cr.exists()) {
                        String suffix = cr.getFilenameExtension();
                        if (suffix != null && suffix.equals("sql")) {
                            String fileName = cr.getFileName();
                            if (LAST_MODIFIED.containsKey(fileName)) {
                                long timestamp = LAST_MODIFIED.get(fileName);
                                long lastModified = cr.getLastModified();
                                if (timestamp != -1 && timestamp != 0 && timestamp != lastModified) {
                                    log.debug("removing expired SQL cache...");
                                    resolveSqlContent(name, cr);
                                    LAST_MODIFIED.put(fileName, lastModified);
                                    log.debug("reload modified sql file:{}", fileName);
                                }
                            }
                        }
                    } else {
                        throw new FileNotFoundException("sql file of name'" + name + "' not found!");
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 初始化加载sql到缓存中
     *
     * @throws IOException        IOExp
     * @throws URISyntaxException URIExp
     */
    public void init() throws IOException, URISyntaxException {
        Map<String, String> mappedPaths = allPaths();
        for (String name : mappedPaths.keySet()) {
            FileResource cr = new FileResource(mappedPaths.get(name));
            if (cr.exists()) {
                String suffix = cr.getFilenameExtension();
                if (suffix != null && suffix.equals("sql")) {
                    resolveSqlContent(name, cr);
                    LAST_MODIFIED.put(cr.getFileName(), cr.getLastModified());
                }
            } else {
                throw new FileNotFoundException("sql file of name'" + name + "' not found!");
            }
        }
    }

    /**
     * 查看sql资源
     */
    public void look() {
        RESOURCE.forEach((k, v) -> System.out.println("\033[95m" + k + "\033[0m -> " + v));
    }

    /**
     * 获取一条sql
     *
     * @param name sql名
     * @return sql
     * @throws IOException        IOExp
     * @throws URISyntaxException URIExp
     */
    public String get(String name) throws IOException, URISyntaxException {
        if (checkModified) {
            reloadIfNecessary();
        }
        if (RESOURCE.containsKey(name)) {
            return RESOURCE.get(name);
        }
        throw new NoSuchElementException(String.format("no SQL named [%s] was found.", name));
    }

    /**
     * 是否在每次获取sql时都检查文件是否更新
     *
     * @param checkModified 是否检查更新
     */
    public void setCheckModified(boolean checkModified) {
        this.checkModified = checkModified;
    }
}
