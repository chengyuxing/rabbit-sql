package rabbit.sql.utils;

import rabbit.common.tuple.Pair;
import rabbit.sql.dao.Wrap;
import rabbit.sql.types.Ignore;
import rabbit.sql.types.Param;
import rabbit.sql.types.ParamMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
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
    public static final Pattern ARG_PATTERN = Pattern.compile("[^:'\"|]:(?<name>[\\w." + SEP + "]+)", Pattern.MULTILINE);
    /**
     * 匹配字符串单引号里面的内容
     */
    public static final Pattern CHILD_STR_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);

    /**
     * 忽略值过滤器
     *
     * @param data   数据
     * @param ignore 忽略类型
     * @return 过滤器
     */
    public static Predicate<String> ignoreValueFilter(final Map<String, Param> data, Ignore ignore) {
        return k -> {
            if (ignore == Ignore.NULL) {
                return data.get(k).getValue() != null;
            }
            if (ignore == Ignore.BLANK) {
                return data.get(k).getValue() != null && !data.get(k).getValue().equals("");
            }
            return true;
        };
    }

    /**
     * 构建一个插入语句
     *
     * @param tableName 表名
     * @param data      数据
     * @param ignore    忽略类型
     * @return 插入语句
     */
    public static String generateInsert(final String tableName, final Map<String, Param> data, final Ignore ignore) {
        String fields = data.keySet().stream()
                .filter(ignoreValueFilter(data, ignore))
                .collect(Collectors.joining(",", "(", ")"));
        String holders = data.keySet().stream()
                .filter(ignoreValueFilter(data, ignore))
                .map(key -> ":" + key)
                .collect(Collectors.joining(",", "(", ")"));
        return "insert into " + tableName + fields + " values " + holders;
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
                .collect(Collectors.joining(", "));
        return "update " + tableName + " set " + sets;
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
     * 去除sql结尾的分号
     *
     * @param sql sql字符串
     * @return 去除分号后的sql
     */
    public static String trimSem(String sql) {
        if (sql.lastIndexOf(";") == -1) {
            return sql;
        }
        sql = sql.substring(0, sql.length() - 1);
        return trimSem(sql);
    }

    /**
     * 解析SQL字符串
     *
     * @param sourceSql 原始sql字符串
     * @param args      参数
     * @return 替换模版占位符后的sql
     */
    public static String resolveSqlPart(final String sourceSql, Map<String, Param> args) {
        AtomicReference<String> sourceSqlRef = new AtomicReference<>(sourceSql);
        args.keySet().forEach(k -> {
            ParamMode pm = args.get(k).getParamMode();
            if (pm == ParamMode.TEMPLATE) {
                String v = args.get(k).getValue().toString() + " ";
                sourceSqlRef.set(sourceSqlRef.get().replace("${" + k + "}", v));
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
}
