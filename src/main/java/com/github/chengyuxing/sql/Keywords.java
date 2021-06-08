package com.github.chengyuxing.sql;

/**
 * 数据库关键字帮助类
 */
public class Keywords {
    public static final String[] STANDARD = new String[]{
            "abort", "absolute", "access", "action", "add", "admin", "after", "aggregate", "all",
            "also", "alter", "always", "analyse", "analyze", "and", "any", "array", "as", "asc",
            "assertion", "assignment", "asymmetric", "at", "attach", "attribute", "authorization",
            "backward", "before", "begin", "between", "bigint", "binary", "bit", "boolean", "both",
            "by", "cache", "call", "called", "cascade", "cascaded", "case", "cast", "catalog",
            "chain", "char", "character", "characteristics", "check", "checkpoint", "class", "close",
            "cluster", "coalesce", "collate", "collation", "column", "columns", "comment", "comments",
            "commit", "committed", "concurrently", "configuration", "conflict", "connection",
            "constraint", "constraints", "content", "continue", "conversion", "copy", "cost", "create",
            "cross", "csv", "cube", "current", "current_catalog", "current_date", "current_role",
            "current_schema", "current_time", "current_timestamp", "current_user", "cursor", "cycle",
            "data", "database", "day", "deallocate", "dec", "decimal", "declare", "default", "defaults",
            "deferrable", "deferred", "definer", "delete", "delimiter", "delimiters", "depends", "desc",
            "detach", "dictionary", "disable", "discard", "distinct", "do", "document", "domain",
            "double", "drop", "each", "else", "enable", "encoding", "encrypted", "end", "enum", "escape",
            "event", "except", "exclude", "excluding", "exclusive", "execute", "exists", "explain",
            "expression", "extension", "external", "extract", "false", "family", "fetch", "filter",
            "first", "float", "following", "for", "force", "foreign", "forward", "freeze", "from", "full",
            "function", "functions", "generated", "global", "grant", "granted", "greatest", "group",
            "grouping", "groups", "handler", "having", "header", "hold", "hour", "identity", "if", "ilike",
            "immediate", "immutable", "implicit", "import", "in", "include", "including", "increment",
            "index", "indexes", "inherit", "inherits", "initially", "inline", "inner", "inout", "input",
            "insensitive", "insert", "instead", "int", "integer", "intersect", "interval", "into",
            "invoker", "is", "isnull", "isolation", "join", "key", "label", "language", "large", "last",
            "lateral", "leading", "leakproof", "least", "left", "level", "like", "limit", "listen",
            "load", "local", "localtime", "localtimestamp", "location", "lock", "locked", "logged",
            "mapping", "match", "materialized", "maxvalue", "method", "minute", "minvalue", "mode",
            "month", "move", "name", "names", "national", "natural", "nchar", "new", "next", "nfc",
            "nfd", "nfkc", "nfkd", "no", "none", "normalize", "normalized", "not", "nothing", "notify",
            "notnull", "nowait", "null", "nullif", "nulls", "numeric", "object", "of", "off", "offset",
            "oids", "old", "on", "only", "operator", "option", "options", "or", "order", "ordinality",
            "others", "out", "outer", "over", "overlaps", "overlay", "overriding", "owned", "owner",
            "parallel", "parser", "partial", "partition", "passing", "password", "placing", "plans",
            "policy", "position", "preceding", "precision", "prepare", "prepared", "preserve", "primary",
            "prior", "privileges", "procedural", "procedure", "procedures", "program", "publication",
            "quote", "range", "read", "real", "reassign", "recheck", "recursive", "ref", "references",
            "referencing", "refresh", "reindex", "relative", "release", "rename", "repeatable", "replace",
            "replica", "reset", "restart", "restrict", "returning", "returns", "revoke", "right", "role",
            "rollback", "rollup", "routine", "routines", "row", "rows", "rule", "savepoint", "schema",
            "schemas", "scroll", "search", "second", "security", "select", "sequence", "sequences",
            "serializable", "server", "session", "session_user", "set", "setof", "sets", "share", "show",
            "similar", "simple", "skip", "smallint", "snapshot", "some", "sql", "stable", "standalone",
            "start", "statement", "statistics", "stdin", "stdout", "storage", "stored", "strict", "strip",
            "subscription", "substring", "support", "symmetric", "sysid", "system", "table", "tables",
            "tablesample", "tablespace", "temp", "template", "temporary", "text", "then", "ties", "time",
            "timestamp", "to", "trailing", "transaction", "transform", "treat", "trigger", "trim", "true",
            "truncate", "trusted", "type", "types", "uescape", "unbounded", "uncommitted", "unencrypted",
            "union", "unique", "unknown", "unlisten", "unlogged", "until", "update", "user", "using",
            "vacuum", "valid", "validate", "validator", "value", "values", "varchar", "variadic", "varying",
            "verbose", "version", "view", "views", "volatile", "when", "where", "whitespace", "window",
            "with", "within", "without", "work", "wrapper", "write", "xml", "xmlattributes", "xmlconcat",
            "xmlelement", "xmlexists", "xmlforest", "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot",
            "xmlserialize", "xmltable", "year", "yes", "zone"
    };

    public static String[] POSTGRESQL = new String[]{
            "raise", "notice", "slice", "loop", "json", "jsonb", "record", "elsif", "minutes", "days", "hours",
            "seconds", "foreach", "exception", "return", "internal", "bytea"
    };

    public static String[] FUNCTIONS = new String[]{
            "sum", "count", "round", "row_number", "rank", "unnest", "date_trunc", "trunc", "string_to_array", "substr",
            "substring", "length", "array_agg", "to_char", "to_date", "to_timestamp", "quote_literal", "jsonb_build_object",
            "jsonb_path_query", "gen_random_uuid", "version", "generate_series", "jsonb_build_array", "max", "greatest",
            "extract", "age", "array_positions", "array_cat", "jsonb_pretty", "array_length", "avg", "min", "dense_rank",
            "percent_rank", "quote_ident", "format", "pg_get_userbyid", "array_upper", "array_lower", "upper", "lower",
            "uuid_generate_v1mc", "trim", "regexp_matches", "regexp_match", "string_agg", "arraycontains", "array_remove",
            "to_number", "array_prepend", "array_append", "lpad", "rpad", "regexp_replace", "regexp_split_to_array",
            "regexp_split_to_table", "ceil", "concat"
    };
}
