package rabbit.sql.utils;

import rabbit.common.tuple.Pair;
import rabbit.common.types.CExpression;
import rabbit.sql.dao.Wrap;
import rabbit.sql.types.Ignore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
     * @param data      数据
     * @param ignore    忽略类型
     * @return 插入语句
     */
    public static String generateInsert(final String tableName, final Map<String, Object> data, final Ignore ignore) {
        String[] fh = data.keySet().stream()
                .filter(k -> {
                    if (ignore == Ignore.NULL) {
                        return data.get(k) != null;
                    }
                    if (ignore == Ignore.BLANK) {
                        return data.get(k) != null && !data.get(k).equals("");
                    }
                    return true;
                }).reduce(new String[]{"", ""}, (acc, current) -> {
                    acc[0] += current + ", ";
                    acc[1] += ":" + current + ", ";
                    return acc;
                }, (a, b) -> a);
        return "insert into " + tableName + "(" + fh[0].substring(0, fh[0].length() - 2) + ") \nvalues (" + fh[1].substring(0, fh[1].length() - 2) + ")";
    }

    /**
     * 构建一个更新语句
     *
     * @param tableName 表名
     * @param data      数据
     * @return 更新语句
     */
    public static String generateUpdate(String tableName, Map<String, Object> data) {
        String sets = data.keySet().stream()
                .filter(key -> !key.contains(SEP))
                .map(key -> key + " = :" + key)
                .collect(Collectors.joining(", \n\t"));
        return "update " + tableName + " \nset " + sets;
    }

    /**
     * 获取处理参数占位符预编译的SQL
     *
     * @param sql 带参数占位符的SQL
     * @return 预编译SQL和参数名的集合
     */
    public static Pair<String, List<String>> getPreparedSql(final String sql) {
        // 首先处理一下sql，先暂时将sql中的单引号内子字符串去除
        String _sql = sql;
        List<String> strChildren = new ArrayList<>();
        Matcher m1 = CHILD_STR_PATTERN.matcher(_sql);
        // 检查是否有子字符串
        int x = 0;
        while (m1.find()) {
            String strChild = m1.group();
            // 为每个子字符串按顺序编号并放入缓存内
            // 此处替换为特殊的占位符防止和用户写的冲突
            _sql = _sql.replace(strChild, SEP + (x++) + SEP);
            strChildren.add(strChild);
        }
        // 开始安全的处理参数占位符
        Matcher matcher = ARG_PATTERN.matcher(_sql);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            String name = matcher.group("name");
            names.add(name);
            // 只替换第一个的原因是为防止参数名的包含关系
            _sql = _sql.replaceFirst(":" + name, "?");
        }

        // 最后再将一开始移除的子字符串重新放回到sql中
        for (int i = 0; i < strChildren.size(); i++) {
            _sql = _sql.replace(SEP + i + SEP, strChildren.get(i));
        }

        return Pair.of(_sql, names);
    }

    /**
     * 如果是值包装类型，就解除包装获取真实值
     *
     * @param value 值或包装类型值
     * @return 真实值
     */
    public static Object unwrapValue(Object value) {
        if (value instanceof Wrap) {
            return ((Wrap) value).getValue();
        }
        return value;
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
            } else {
                Object v = args.get(key);
                if (v instanceof Wrap) {
                    Wrap wrapV = (Wrap) v;
                    sql = sql.replace(":" + key, wrapV.getStart() + " :" + key + wrapV.getEnd());
                }
            }
        }
        return sql;
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
        String[] lines = sql.split("\n");
        StringBuilder sb = new StringBuilder();
        String firstLine = "";
        boolean first = true;
        boolean skip = true;
        boolean start = false;
        for (String line : lines) {
            String trimLine = line.trim();
            if (first) {
                firstLine = trimLine;
                first = false;
            }
            if (trimLine.startsWith("--#if") && !start) {
                String filter = trimLine.substring(5);
                CExpression expression = CExpression.of(filter);
                skip = expression.calc(argsMap);
                start = true;
                continue;
            }
            if (trimLine.startsWith("--#fi") && start) {
                skip = true;
                start = false;
                continue;
            }
            if (skip) {
                sb.append(line).append("\n");
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
