package rabbit.sql.utils;

import rabbit.common.tuple.Pair;
import rabbit.common.types.CExpression;
import rabbit.sql.types.Ignore;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL工具类
 */
public class SqlUtil {
    /**
     * 特殊字符用来防止字段名重复的问题
     */
    public static final String SEP = "\u02de";
    /**
     * 匹配命名参数
     */
    public static final Pattern ARG_PATTERN = Pattern.compile("(^:|[^:]:)(?<name>[\\w." + SEP + "]+)", Pattern.MULTILINE);
    /**
     * 匹配字符串单引号里面的内容
     */
    public static final Pattern CHILD_STR_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);

    /**
     * 构建一个插入语句
     *
     * @param tableName 表名
     * @param row       数据
     * @param ignore    忽略类型
     * @param fields    需要包含的字段集合
     * @return 插入语句
     */
    public static String generateInsert(final String tableName, final Map<String, Object> row, final Ignore ignore, List<String> fields) {
        Set<String> keys = row.keySet();
        if (fields != null && !fields.isEmpty()) {
            Iterator<String> keyIterator = keys.iterator();
            while (keyIterator.hasNext()) {
                if (!fields.contains(keyIterator.next())) {
                    keyIterator.remove();
                }
            }
        }

        Iterator<String> keyIterator = keys.iterator();
        if (ignore == Ignore.NULL) {
            while (keyIterator.hasNext()) {
                if (row.get(keyIterator.next()) == null) {
                    keyIterator.remove();
                }
            }
        } else if (ignore == Ignore.BLANK) {
            while (keyIterator.hasNext()) {
                String k = keyIterator.next();
                if (row.get(k) == null || row.get(k).equals("")) {
                    keyIterator.remove();
                }
            }
        }

        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate insert sql error.");
        }

        StringBuilder f = new StringBuilder();
        StringBuilder h = new StringBuilder();
        for (String key : keys) {
            f.append(key).append(", ");
            h.append(":").append(key).append(", ");
        }
        return "insert into " + tableName + "(" + f.substring(0, f.length() - 2) + ") \nvalues (" + h.substring(0, h.length() - 2) + ")";
    }

    /**
     * 构建一个更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @return 更新语句
     */
    public static String generateUpdate(String tableName, Map<String, Object> data) {
        Set<String> keys = data.keySet();
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            if (!key.contains(SEP)) {
                sb.append(key).append(" = :").append(key).append(",\n\t");
            }
        }
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("empty field set, generate update sql error.");
        }
        return "update " + tableName + " \nset " + sb.substring(0, sb.lastIndexOf(","));
    }

    /**
     * 处理一段sql，将sql内出现的字符串（单引号包裹的部分）替换为一个特殊的占位符，并将替换的字符串存储下来，可对原sql进行一些其他处理操作，而不受到字符串内容的影响
     *
     * @param sql sql字符串
     * @return 替换字符串后带有特殊占位符的sql和占位符与字符串的映射
     */
    public static Pair<String, Map<String, String>> replaceStrPartOfSql(final String sql) {
        String noneStrSql = sql;
        Map<String, String> mapper = new HashMap<>();
        Matcher m = CHILD_STR_PATTERN.matcher(sql);
        int x = 0;
        while (m.find()) {
            // sql part of string
            String str = m.group();
            // indexed placeholder
            String placeHolder = SEP + (x++) + SEP;
            noneStrSql = noneStrSql.replace(str, placeHolder);
            mapper.put(placeHolder, str);
        }
        return Pair.of(noneStrSql, mapper);
    }

    /**
     * 获取处理参数占位符预编译的SQL
     *
     * @param sql 带参数占位符的SQL
     * @return 预编译SQL和参数名的集合
     */
    public static Pair<String, List<String>> getPreparedSql(final String sql) {
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceStrPartOfSql(sql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        // safe to replace arg by name placeholder
        Matcher matcher = ARG_PATTERN.matcher(noneStrSql);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            String name = matcher.group("name");
            names.add(name);
            noneStrSql = noneStrSql.replaceFirst(":" + name, "?");
        }
        // finally set placeholder into none-string-part sql
        for (String key : placeholderMapper.keySet()) {
            noneStrSql = noneStrSql.replace(key, placeholderMapper.get(key));
        }
        return Pair.of(noneStrSql, names);
    }

    /**
     * 排除sql字符串尾部的非sql语句部分的其他字符
     *
     * @param sql sql字符串
     * @return 去除分号后的sql
     */
    public static String trimEnd(String sql) {
        return sql.replaceAll("([\\s;]*)$", "");
    }

    /**
     * 解析SQL字符串
     *
     * @param sourceSql 原始sql字符串
     * @param args      参数
     * @return 替换模版占位符后的sql
     */
    public static String resolveSqlPart(final String sourceSql, Map<String, Object> args) {
        if (args == null || args.size() == 0) {
            return sourceSql;
        }
        String sql = sourceSql;
        for (String key : args.keySet()) {
            if (key.startsWith("${") && key.endsWith("}")) {
                String v = " " + args.get(key).toString() + " ";
                sql = sql.replace(key, v);
            }
        }
        return sql;
    }

    /**
     * 移除sql的块注释
     *
     * @param sql sql
     * @return 去除块注释的sql
     */
    public static String removeAnnotationBlock(final String sql) {
        Pair<String, Map<String, String>> noneStrSqlAndHolder = replaceStrPartOfSql(sql);
        String noneStrSql = noneStrSqlAndHolder.getItem1();
        Map<String, String> placeholderMapper = noneStrSqlAndHolder.getItem2();
        char[] chars = noneStrSql.toCharArray();
        List<Character> characters = new ArrayList<>();
        int count = 0;
        for (int i = 0; i < chars.length; i++) {
            int prev = i;
            int next = i;
            if (i > 0) {
                prev = i - 1;
            }
            if (i < chars.length - 1) {
                next = i + 1;
            }
            if (chars[i] == '/' && chars[next] == '*') {
                count++;
            } else if (chars[i] == '*' && chars[next] == '/') {
                count--;
            } else if (count == 0) {
                if (chars[prev] != '*' && chars[i] != '/') {
                    characters.add(chars[i]);
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        for (Character c : characters) {
            sb.append(c);
        }
        String noneBSql = sb.toString().replaceAll("\n\\s*\n", "\n");
        for (String key : placeholderMapper.keySet()) {
            noneBSql = noneBSql.replace(key, placeholderMapper.get(key));
        }
        return noneBSql;
    }

    /**
     * 根据解析条件表达式的结果动态生成sql<br>
     * e.g. data.sql.template
     *
     * @param sql     sql
     * @param argsMap 参数字典
     * @return 解析后的sql
     * @see CExpression
     */
    public static String dynamicSql(final String sql, Map<String, Object> argsMap) {
        if (argsMap == null || argsMap.isEmpty()) {
            return sql;
        }
        if (!sql.contains("--#if") || !sql.contains("--#fi")) {
            return sql;
        }
        String nSql = removeAnnotationBlock(sql);
        String[] lines = nSql.split("\n");
        StringBuilder sb = new StringBuilder();
        String firstLine = "";
        boolean first = true;
        boolean ok = true;
        boolean start = false;
        boolean inBlock = false;
        boolean blockFirstOk = false;
        for (String line : lines) {
            String trimLine = line.trim();
            if (!trimLine.isEmpty()) {
                if (first) {
                    if (!trimLine.startsWith("--")) {
                        firstLine = trimLine;
                        first = false;
                    }
                }
                if (trimLine.startsWith("--#choose")) {
                    inBlock = true;
                    continue;
                }
                if (trimLine.startsWith("--#end")) {
                    inBlock = false;
                    continue;
                }
                if (trimLine.startsWith("--#if") && !start) {
                    start = true;
                    if (inBlock) {
                        if (!blockFirstOk) {
                            String filter = trimLine.substring(5);
                            CExpression expression = CExpression.of(filter);
                            ok = expression.calc(argsMap);
                            blockFirstOk = ok;
                        } else {
                            ok = false;
                        }
                    } else {
                        String filter = trimLine.substring(5);
                        CExpression expression = CExpression.of(filter);
                        ok = expression.calc(argsMap);
                    }
                    continue;
                }
                if (trimLine.startsWith("--#fi") && start) {
                    ok = true;
                    start = false;
                    continue;
                }
                if (ok) {
                    sb.append(line).append("\n");
                    if (!inBlock) {
                        blockFirstOk = false;
                    }
                }
            }
        }
        String dSql = sb.toString();
        Pattern p;
        Matcher m;
        // if update statement
        if (firstLine.startsWith("update")) {
            p = Pattern.compile(",\\s*where", Pattern.CASE_INSENSITIVE);
            m = p.matcher(dSql);
            if (m.find()) {
                dSql = dSql.substring(0, m.start()).concat(dSql.substring(m.start() + 1));
            }
        }
        // "where and" statement
        p = Pattern.compile("where\\s+(and|or)\\s+", Pattern.CASE_INSENSITIVE);
        m = p.matcher(dSql);
        if (m.find()) {
            return dSql.substring(0, m.start() + 6).concat(dSql.substring(m.end()));
        }
        // if "where order by ..." statement
        p = Pattern.compile("where\\s+(order by|limit|group by|union)\\s+", Pattern.CASE_INSENSITIVE);
        m = p.matcher(dSql);
        if (m.find()) {
            return dSql.substring(0, m.start()).concat(dSql.substring(m.start() + 6));
        }
        // if "where" at end
        p = Pattern.compile("where\\s*$", Pattern.CASE_INSENSITIVE);
        m = p.matcher(dSql);
        if (m.find()) {
            return dSql.substring(0, m.start());
        }
        return dSql;
    }

    /**
     * 通过查询sql大致的构建出查询条数的sql
     *
     * @param recordQuery 查询sql
     * @return 条数查询sql
     */
    public static String generateCountQuery(final String recordQuery) {
        // for 0 errors, sorry!!!
        return "select count(*) from (" + recordQuery + ") _data";
    }
}
