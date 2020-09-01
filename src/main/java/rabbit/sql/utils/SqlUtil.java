package rabbit.sql.utils;

import rabbit.common.tuple.Pair;
import rabbit.common.types.CExpression;
import rabbit.sql.dao.Wrap;
import rabbit.sql.types.Ignore;
import rabbit.sql.types.Param;
import rabbit.sql.types.ParamMode;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
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
    public static final Pattern ARG_PATTERN = Pattern.compile("[^:]:(?<name>[\\w." + SEP + "]+)", Pattern.MULTILINE);
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
    public static String generateInsert(final String tableName, final Map<String, Param> data, final Ignore ignore) {
        String[] fh = data.keySet().stream()
                .filter(k -> {
                    if (ignore == Ignore.NULL) {
                        return data.get(k).getValue() != null;
                    }
                    if (ignore == Ignore.BLANK) {
                        return data.get(k).getValue() != null && !data.get(k).getValue().equals("");
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
    public static String generateUpdate(String tableName, Map<String, Param> data) {
        String sets = data.keySet().stream()
                .filter(key -> !key.contains(SEP))
                .map(key -> key + " = :" + key)
                .collect(Collectors.joining(", \n\t"));
        return "update " + tableName + " \nset " + sets;
    }

    /**
     * 解析获取预编译的SQL和按顺序的参数占位符名字
     *
     * @param sql 带参数占位符的SQL
     * @return 预编译SQL和参数名的集合
     */
    public static Pair<String, List<String>> getPreparedSqlAndIndexedArgNames(final String sql) {
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
    public static String resolveSqlPart(final String sourceSql, Map<String, Param> args) {
        if (args == null || args.size() == 0) {
            return sourceSql;
        }
        AtomicReference<String> sourceSqlRef = new AtomicReference<>(sourceSql);
        args.keySet().forEach(k -> {
            ParamMode pm = args.get(k).getParamMode();
            if (pm == ParamMode.TEMPLATE) {
                String sql = sourceSqlRef.get();
                String v = args.get(k).getValue().toString() + " ";
                String key = "${" + k + "}";
                sourceSqlRef.set(sql.replace(key, v));
            } else if (pm == ParamMode.IN || pm == ParamMode.IN_OUT) {
                Object v = args.get(k).getValue();
                if (v instanceof Wrap) {
                    Wrap wrapV = (Wrap) v;
                    sourceSqlRef.set(sourceSqlRef.get().replace(":" + k, wrapV.getStart() + " :" + k + wrapV.getEnd()));
                }
            }
        });
        return sourceSqlRef.get();
    }

    /**
     * 根据解析条件表达式的结果动态生成sql<br>
     * e.g. data.sql.template
     *
     * @param sql       sql
     * @param paramsMap 参数字典
     * @return 解析后的sql
     * @see CExpression
     */
    public static String dynamicSql(String sql, Map<String, Param> paramsMap) {
        if (!sql.contains("--#if") || !sql.contains("--#fi")) {
            return sql;
        }
        if (paramsMap == null || paramsMap.size() == 0) {
            return sql;
        }
        Map<String, Object> params = paramsMap.keySet().stream()
                .filter(k -> paramsMap.get(k).getParamMode() == ParamMode.IN)
                .collect(HashMap::new,
                        (current, k) -> current.put(k, SqlUtil.unwrapValue(paramsMap.get(k).getValue())),
                        HashMap::putAll);
        String[] lines = sql.split("\n");
        StringBuilder sb = new StringBuilder();
        boolean skip = true;
        boolean start = false;
        for (String line : lines) {
            String trimLine = line.trim();
            if (trimLine.startsWith("--#if") && !start) {
                String filter = trimLine.substring(5);
                CExpression expression = CExpression.of(filter);
                skip = expression.getResult(params);
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
        // update statement
        Pattern p = Pattern.compile(",\\s*where", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(dSql);
        if (m.find()) {
            return dSql.substring(0, m.start()).concat(dSql.substring(m.start() + 1));
        }
        // where and statement
        p = Pattern.compile("where\\s*and", Pattern.CASE_INSENSITIVE);
        m = p.matcher(dSql);
        if (m.find()) {
            return dSql.substring(0, m.start() + 5).concat(dSql.substring(m.end()));
        }
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
        /// part of from to end
        // temp sql to get keyword index only.
        String tempRql = recordQuery.toLowerCase().trim();
        String from2end = recordQuery.substring(tempRql.lastIndexOf("from"));
        String select2from = "select count(*) ";

        // if target table is child query
        Pattern childP = Pattern.compile("^select\\s*[\\S\\s]*(?<fromIdx>from)\\s*\\(");
        Matcher childM = childP.matcher(tempRql);
        if (childM.find()) {
            from2end = recordQuery.substring(childM.start("fromIdx"));
            // if is common expression (with).
        } else if (tempRql.startsWith("with")) {
            Pattern p = Pattern.compile("^\\s*with[\\s\\S]+\\s+as\\s*\\([\\s\\S]+\\)\\s*select");
            Matcher m = p.matcher(tempRql);
            if (m.find()) {
                select2from = recordQuery.substring(0, m.end()) + " count(*) ";
            }
        }

        // if is PostgreSQL lateral statement
        if (tempRql.contains("lateral")) {
            Pattern lateralP = Pattern.compile(",\\s*(?<lateralIdx>lateral)\\s*\\(select");
            Matcher lateralM = lateralP.matcher(tempRql);
            if (lateralM.find()) {
                int fromIdx = tempRql.lastIndexOf("from", lateralM.start("lateralIdx"));
                from2end = recordQuery.substring(tempRql.lastIndexOf("from", fromIdx));
            }
        }

        /// part of select to from
        String countQuery = select2from + from2end;
        // if has order by statement
        int orderByIdx = countQuery.toLowerCase().lastIndexOf("order by");
        if (orderByIdx != -1) {
            countQuery = countQuery.substring(0, orderByIdx);
        }
        return countQuery;
    }
}
