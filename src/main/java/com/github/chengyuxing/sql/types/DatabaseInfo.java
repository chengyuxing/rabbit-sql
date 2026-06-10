package com.github.chengyuxing.sql.types;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

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

    public static DatabaseInfo of(Connection connection) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            return new DatabaseInfo(
                    metaData.getDatabaseProductName().toLowerCase(),
                    metaData.getDatabaseProductVersion(),
                    metaData.getURL(),
                    metaData.getIdentifierQuoteString(),
                    metaData.getDriverName()
            );
        } catch (SQLException e) {
            throw new IllegalStateException("Fetch database metadata failed", e);
        }
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
