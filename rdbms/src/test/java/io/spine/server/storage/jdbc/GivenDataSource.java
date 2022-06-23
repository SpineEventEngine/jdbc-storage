/*
 * Copyright 2022, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.spine.server.storage.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

import static io.spine.base.Identifier.newUuid;

public class GivenDataSource {

    /**
     * The URL prefix of an in-memory HyperSQL DB.
     */
    //TODO:2017-09-13:dmytro.grankin: Enable flexible configuration of drivers.
    // See the details: https://github.com/SpineEventEngine/jdbc-storage/issues/37
    private static final String HSQL_IN_MEMORY_DB_URL_PREFIX = "jdbc:h2:mem:";

    private static final String UNSUPPORTED = "Operation unsupported.";

    private GivenDataSource() {
    }

    public static DataSourceWrapper whichIsStoredInMemory(String dbName) {
        HikariConfig config = hikariConfig(dbName);
        DataSourceWrapper dataSource = DataSourceWrapper.wrap(new HikariDataSource(config));
        return dataSource;
    }

    public static DataSourceWrapper
    whichHoldsMetadata(String productName, int majorVersion, int minorVersion) {
        DataSourceWrapper dataSource =
                new DataSourceWithMetaData(productName, majorVersion, minorVersion);
        return dataSource;
    }

    public static ThrowingHikariDataSource whichIsThrowingByCommand(String dbName) {
        HikariConfig config = hikariConfig(dbName);
        ThrowingHikariDataSource dataSource = new ThrowingHikariDataSource(config);
        return dataSource;
    }

    private static HikariConfig hikariConfig(String dbName) {
        HikariConfig config = new HikariConfig();
        String dbUrl = prefix(dbName);
        config.setJdbcUrl(dbUrl);
        // Not setting username and password is OK for in-memory database.
        return config;
    }

    public static String prefix(String dbNamePrefix) {
        return HSQL_IN_MEMORY_DB_URL_PREFIX + dbNamePrefix + newUuid();
    }

    /**
     * A test data source whose only purpose is to return a given data source
     * {@linkplain DataSourceWrapper#metaData() metadata}.
     */
    private static class DataSourceWithMetaData implements DataSourceWrapper {

        private final String productName;
        private final int majorVersion;
        private final int minorVersion;

        private DataSourceWithMetaData(String productName, int majorVersion, int minorVersion) {
            this.productName = productName;
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        @Override
        public ConnectionWrapper getConnection(boolean autoCommit) {
            throw new IllegalStateException(UNSUPPORTED);
        }

        @Override
        public DataSourceMetaData metaData() throws DatabaseException {
            return new DataSourceMetaData() {
                @Override
                public String productName() {
                    return productName;
                }

                @Override
                public int majorVersion() {
                    return majorVersion;
                }

                @Override
                public int minorVersion() {
                    return minorVersion;
                }
            };
        }

        @Override
        public void close() throws DatabaseException {
            throw new IllegalStateException(UNSUPPORTED);
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    public static class ThrowingHikariDataSource extends HikariDataSource {

        private boolean throwOnGetConnection;
        private boolean throwOnClose;

        private ThrowingHikariDataSource(HikariConfig configuration) {
            super(configuration);
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (throwOnGetConnection) {
                throw new SQLException("Ignore this SQL exception.");
            }
            return super.getConnection();
        }

        @Override
        public void close() {
            if (throwOnClose) {
                throw new IllegalStateException("Ignore this error.");
            }
            super.close();
        }

        public void setThrowOnGetConnection(boolean throwOnGetConnection) {
            this.throwOnGetConnection = throwOnGetConnection;
        }

        public void setThrowOnClose(boolean throwOnClose) {
            this.throwOnClose = throwOnClose;
        }
    }
}
