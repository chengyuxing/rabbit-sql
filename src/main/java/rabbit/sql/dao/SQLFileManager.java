package rabbit.sql.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.common.io.FileResource;
import rabbit.common.utils.ResourceUtil;
import rabbit.common.utils.StringUtil;
import rabbit.sql.utils.SqlUtil;

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
    private final AtomicInteger UN_NAMED_SQL_INDEX = new AtomicInteger();
    private final String UN_NAMED_SQL_NAME = "UN_NAMED_SQL_";
    private static final String[] EXPRESSION_KEYWORDS = new String[]{
            "--#if",
            "--#fi",
            "--#choose",
            "--#end"
    };
    private final Map<String, Long> LAST_MODIFIED = new HashMap<>();
    private static final Pattern NAME_PATTERN = Pattern.compile("/\\*\\s*\\[\\s*(?<name>\\S+)\\s*]\\s*\\*/");
    private static final Pattern PART_PATTERN = Pattern.compile("/\\*\\s*\\{\\s*(?<part>\\S+)\\s*}\\s*\\*/");
    private volatile boolean checkModified;
    private String[] sqls;
    private Map<String, String> sqlMap;
    private List<String> sqlList;

    public SQLFileManager() {
    }

    /**
     * Sql文件解析器实例<br>
     * 文件名必须从classpath目录开始到文件名(包含后缀.sql)，多个sql文件以逗号(,)分割
     *
     * @param sqls 多个文件名
     */
    public SQLFileManager(String sqls) {
        this.sqls = sqls.split(",");
    }

    /**
     * 添加命名的sql文件
     *
     * @param alias       sql文件别名
     * @param sqlFileName sql文件全路径名
     */
    public void add(String alias, String sqlFileName) {
        if (sqlMap == null) {
            sqlMap = new HashMap<>();
        }
        sqlMap.put(alias, sqlFileName);
    }

    /**
     * 设置命名的sql文件
     *
     * @param sqlMap 命名sql文件名和路径对应关系
     */
    public void setSqlMap(Map<String, String> sqlMap) {
        this.sqlMap = sqlMap;
    }

    /**
     * 添加未命名的sql文件
     *
     * @param sqlFileName sql文件全路径名
     */
    public void add(String sqlFileName) {
        if (sqlList == null) {
            sqlList = new ArrayList<>();
        }
        sqlList.add(sqlFileName);
    }

    /**
     * 设置未命名的sql文件
     *
     * @param sqlList sql文件全路径列表
     */
    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }

    /**
     * 解析sql文件
     *
     * @param name     sql文件自定义名
     * @param resource 类路径sql资源
     * @throws IOException IOexp
     */
    private void resolveSqlContent(String name, FileResource resource) throws IOException {
        Map<String, String> singleResource = new HashMap<>();
        String pkgPath = ResourceUtil.path2package(resource.getPath());
        String prefix = pkgPath.substring(0, pkgPath.length() - 3);
        if (!name.startsWith(UN_NAMED_SQL_NAME)) {
            prefix = name + ".";
        }
        String previousSqlName = "";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimLine = line.trim();
                if (!trimLine.isEmpty()) {
                    Matcher m_name = NAME_PATTERN.matcher(trimLine);
                    Matcher m_part = PART_PATTERN.matcher(trimLine);
                    if (m_name.matches()) {
                        previousSqlName = prefix + m_name.group("name");
                        if (singleResource.containsKey(previousSqlName)) {
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
                        // exclude single line annotation except expression keywords
                        if (!trimLine.startsWith("--") || StringUtil.startsWiths(trimLine, EXPRESSION_KEYWORDS)) {
                            if (!previousSqlName.equals("")) {
                                String prepareLine = singleResource.get(previousSqlName) + line;
                                if (trimLine.endsWith(";")) {
                                    String naSql = SqlUtil.removeAnnotationBlock(prepareLine);
                                    singleResource.put(previousSqlName, naSql.substring(0, naSql.lastIndexOf(";")));
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
            // if last part of sql is not ends with ';' symbol
            if (!previousSqlName.equals("")) {
                String lastSql = singleResource.get(previousSqlName);
                singleResource.put(previousSqlName, SqlUtil.removeAnnotationBlock(lastSql));
                log.debug("scan to get SQL [{}]：{}", previousSqlName, lastSql);
            }
        }
        mergeSqlPartIfNecessary(singleResource, prefix);
        RESOURCE.putAll(singleResource);
    }

    /**
     * 执行合并SQL片段
     *
     * @param partName    sql片段名
     * @param prefix      sql名前缀，自定义名或sql文件路径名
     * @param sqlResource sql字符串文件资源
     */
    private void doMergeSqlPart(final String partName, final String prefix, Map<String, String> sqlResource) {
        // inner sql part name like: ${filename.part1}
        //innerPartName will be '${part1}'
        String innerPartName = "${" + partName.substring(prefix.length() + 2);
        boolean has = false;
        for (String key : sqlResource.keySet()) {
            String sql = sqlResource.get(key);
            if (sql.contains(innerPartName)) {
                String part = sqlResource.get(partName);
                sqlResource.put(key, sql.replace(innerPartName, part));
            }
            if (sqlResource.get(key).contains(innerPartName)) {
                has = true;
            }
        }
        // go on if inner part name still exists
        if (has) {
            doMergeSqlPart(partName, prefix, sqlResource);
        }
    }

    /**
     * 合并SQL可复用片段到包含片段名的SQL中
     *
     * @param sqlResource sql字符串资源
     * @param prefix      sql名前缀，自定义名或sql文件路径名
     */
    private void mergeSqlPartIfNecessary(Map<String, String> sqlResource, String prefix) {
        for (String key : sqlResource.keySet()) {
            if (key.startsWith("${")) {
                doMergeSqlPart(key, prefix, sqlResource);
            }
        }
    }

    /**
     * 获取没有命名的防止重复的自增sql文件路径名
     *
     * @return sql路径名
     */
    private String getUnnamedPathName() {
        return UN_NAMED_SQL_NAME + UN_NAMED_SQL_INDEX.getAndIncrement();
    }

    /**
     * 整合所有命名和无命名Sql
     *
     * @return 所有sql
     */
    private Map<String, String> allPaths() {
        Map<String, String> pathMap = new HashMap<>();
        // add unnamed paths
        if (sqlList != null && sqlList.size() > 0) {
            for (String path : sqlList) {
                pathMap.put(getUnnamedPathName(), path);
            }
        }
        // add paths from constructor's args
        if (sqls != null && sqls.length > 0) {
            for (String path : sqls) {
                pathMap.put(getUnnamedPathName(), path);
            }
        }
        // add named paths
        if (sqlMap != null && sqlMap.size() > 0) {
            pathMap.putAll(sqlMap);
        }
        return pathMap;
    }

    /**
     * 如果有检测到文件修改过，则重新加载已修改过的sql文件
     */
    private void reloadIfNecessary() throws IOException, URISyntaxException {
        lock.lock();
        try {
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
                        } else {
                            log.debug("load new sql file:{}", fileName);
                            resolveSqlContent(name, cr);
                            LAST_MODIFIED.put(fileName, cr.getLastModified());
                        }
                    }
                } else {
                    throw new FileNotFoundException("sql file of name'" + name + "' not found!");
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
     * 是否已进行过初始化
     *
     * @return 初始化状态
     */
    public boolean isInitialized() {
        return !RESOURCE.isEmpty() && !LAST_MODIFIED.isEmpty();
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
