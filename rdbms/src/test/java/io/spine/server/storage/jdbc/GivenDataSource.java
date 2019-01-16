/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import javax.sql.DataSource;

import static io.spine.base.Identifier.newUuid;
import static org.mockito.Mockito.mock;

/**
 * @author Alexander Litus
 * @author Dmytro Dashenkov
 * @author Dmytro Grankin
 */
public class GivenDataSource {

    /**
     * The URL prefix of an in-memory HyperSQL DB.
     */
    //TODO:2017-09-13:dmytro.grankin: Enable flexible configuration of drivers.
    // See the details: https://github.com/SpineEventEngine/jdbc-storage/issues/37
    private static final String HSQL_IN_MEMORY_DB_URL_PREFIX = "jdbc:h2:mem:";

    private GivenDataSource() {
    }

    public static DataSourceWrapper withoutSuperpowers() {
        return mock(DataSourceWrapper.class);
    }

    public static ClosableDataSource whichIsAutoCloseable() {
        return mock(ClosableDataSource.class);
    }

    public static DataSourceWrapper whichIsStoredInMemory(String dbName) {
        HikariConfig config = new HikariConfig();
        String dbUrl = prefix(dbName);
        config.setJdbcUrl(dbUrl);
        // Not setting username and password is OK for in-memory database.
        DataSourceWrapper dataSource = DataSourceWrapper.wrap(new HikariDataSource(config));
        return dataSource;
    }

    public static String prefix(String dbNamePrefix) {
        return HSQL_IN_MEMORY_DB_URL_PREFIX + dbNamePrefix + newUuid();
    }

    @SuppressWarnings("InterfaceNeverImplemented")
    public interface ClosableDataSource extends DataSource, AutoCloseable {
    }
}
