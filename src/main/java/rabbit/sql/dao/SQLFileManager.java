package rabbit.sql.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.common.utils.ResourceUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQL文件管理器
 */
public final class SQLFileManager {
    private final static Logger log = LoggerFactory.getLogger(LightDao.class);
    private final ReentrantLock lock = new ReentrantLock();
    static final ConcurrentHashMap<String, String> RESOURCE = new ConcurrentHashMap<>();
    final Map<String, Long> LAST_MODIFIED = new HashMap<>();

    private String basePath;
    private boolean checkModified;

    private static final String NAME_OPEN = "/*[";
    private static final String NAME_CLOSE = "]*/";
    private static final String PART_OPEN = "/*{";
    private static final String PART_CLOSE = "}*/";
    private static final String SEPARATOR = ";";

    SQLFileManager() {
    }

    public SQLFileManager(String basePath) {
        this.basePath = basePath;
    }

    private void resolveSqlContent(Path path) throws IOException {
        String fileName = path.getFileName().toString();
        String prefix = fileName.substring(0, fileName.length() - 3);
        String previousSqlName = "";
        boolean firstLine = true;
        BufferedReader reader = Files.newBufferedReader(path);
        String line;
        while ((line = reader.readLine()) != null) {
            String trimLine = line.trim();
            if (!trimLine.isEmpty()) {
                if (firstLine) {
                    if (!trimLine.startsWith(NAME_OPEN) && !trimLine.startsWith(PART_OPEN)) {
                        continue;
                    }
                }
                if (trimLine.startsWith(NAME_OPEN) && trimLine.endsWith(NAME_CLOSE)) {
                    firstLine = false;
                    String sqlName = prefix + line.substring(line.indexOf(NAME_OPEN) + NAME_OPEN.length(), line.indexOf(NAME_CLOSE));
                    previousSqlName = sqlName;
                    RESOURCE.put(sqlName, "");
                } else if (trimLine.startsWith(PART_OPEN) && trimLine.endsWith(PART_CLOSE)) {
                    firstLine = false;
                    String partName = prefix + line.substring(line.indexOf(PART_OPEN) + PART_OPEN.length(), line.indexOf(PART_CLOSE));
                    partName = "${" + partName + "}";
                    previousSqlName = partName;
                    RESOURCE.put(partName, "");
                } else {
                    String prepareLine = RESOURCE.get(previousSqlName) + line;
                    if (trimLine.endsWith(SEPARATOR)) {
                        RESOURCE.put(previousSqlName, prepareLine.substring(0, prepareLine.lastIndexOf(SEPARATOR)));
                        log.debug("scan to get SQL [{}]：{}", previousSqlName, RESOURCE.get(previousSqlName));
                    } else
                        RESOURCE.put(previousSqlName, prepareLine.concat("\n"));
                }
            }
        }
        reader.close();
        mergeSqlPartIfNecessary();
    }

    /**
     * 执行合并SQL片段
     *
     * @param partName sql片段名
     */
    private void doMergeSqlPart(final String partName) {
        String pn = "${" + partName.substring(partName.indexOf(".") + 1);
        boolean has = false;
        for (String key : RESOURCE.keySet()) {
            if (!key.startsWith("${")) {
                if (RESOURCE.get(key).contains(pn)) {
                    RESOURCE.put(key, RESOURCE.get(key).replace(pn, RESOURCE.get(partName)));
                }
                if (RESOURCE.get(key).contains(pn)) {
                    has = true;
                }
            }
        }
        if (has) {
            doMergeSqlPart(partName);
        }
    }

    /**
     * 合并SQL可复用片段到包含片段名的SQL中
     */
    private void mergeSqlPartIfNecessary() {
        RESOURCE.keySet().stream()
                .filter(k -> k.startsWith("${"))
                .forEach(this::doMergeSqlPart);
    }

    /**
     * 如果有检测到文件修改过，则重新加载已修改过的sql文件
     */
    private void reloadIfNecessary() throws IOException, URISyntaxException {
        if (!LAST_MODIFIED.isEmpty())
            ResourceUtil.getClassPathResources(basePath, ".sql")
                    .filter(p -> {
                        File f = p.toFile();
                        // 如果有文件并且文件修改过
                        if (LAST_MODIFIED.containsKey(f.getName())) {
                            long timestamp = LAST_MODIFIED.get(f.getName());
                            return timestamp != f.lastModified();
                        }
                        // 如果是新文件则不过滤
                        return true;
                    }).forEach(p -> {
                File f = p.toFile();
                String name = f.getName();
                log.debug("removing expired SQL cache...");
                RESOURCE.keySet().stream()
                        .filter(k -> k.startsWith(name.substring(0, name.length() - 3)))
                        .forEach(RESOURCE::remove);
                try {
                    resolveSqlContent(p);
                    log.debug("reload modified sql file:{}", name);
                } catch (IOException e) {
                    log.error("resolve SQL file with an error:{}", e.getMessage());
                }
                //更新最后一次修改的时间
                LAST_MODIFIED.put(name, f.lastModified());
            });
    }

    /**
     * 初始化加载sql到缓存中
     *
     * @throws IOException        IOExp
     * @throws URISyntaxException URIExp
     */
    public void init() throws IOException, URISyntaxException {
        ResourceUtil.getClassPathResources(basePath, ".sql")
                .forEach(p -> {
                    try {
                        resolveSqlContent(p);
                    } catch (IOException e) {
                        log.error("resolve SQL file with an error:{}", e.getMessage());
                    }
                    File f = p.toFile();
                    LAST_MODIFIED.put(f.getName(), f.lastModified());
                });
    }

    public void look() {
        RESOURCE.forEach((k, v) -> {
            System.out.println(String.format("KEY:[%s]\nVALUE:[%s]", k, v));
        });
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
            lock.lock();
            reloadIfNecessary();
            lock.unlock();
        }
        if (RESOURCE.containsKey(name)) {
            return RESOURCE.get(name);
        }
        throw new NoSuchElementException(String.format("no SQL named [%s] was found.", name));
    }

    public String getBasePath() {
        return basePath;
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
