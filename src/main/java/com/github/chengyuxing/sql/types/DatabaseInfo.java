package com.github.chengyuxing.sql.types;

public final class DatabaseInfo {
    private final String name;
    private final String version;
    private final String jdbcUrl;
    private final String quote;
    private final String driver;

    public DatabaseInfo(String name, String version, String jdbcUrl, String quote, String driver) {
        this.name = name;
        this.version = version;
        this.jdbcUrl = jdbcUrl;
        this.quote = quote;
        this.driver = driver;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getQuote() {
        return quote;
    }

    public String getDriver() {
        return driver;
    }
}
