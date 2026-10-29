/*
 * Copyright 2026 CodeMatters, Lda.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package io.spine.server.storage.jdbc.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.PredefinedMapping;
import org.jspecify.annotations.Nullable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mysql.MySQLContainer;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_9_7;

/**
 * Defines the common routines to use in tests against MySQL instance.
 *
 * <p>The class is public for {@code MysqlServerVersionSpec}, which must reside in the package
 * of {@link PredefinedMapping} to read the version of a mapping.
 */
public final class MysqlTests {

    /**
     * The Docker image of the MySQL server used in the tests.
     *
     * <p>It is the LTS release the {@linkplain PredefinedMapping#MYSQL_9_7 mapping} is made for;
     * {@code MysqlServerVersionSpec} checks that the two match. The tag pins the release instead
     * of following {@code lts}, which moves to the next LTS release on its own.
     *
     * <p>The image is published for {@code arm64} in addition to {@code amd64}, so the container
     * boots natively on Apple Silicon.
     */
    private static final String IMAGE = "mysql:9.7";

    /**
     * A single MySQL container shared by all the tests in this package.
     *
     * <p>Booting a MySQL container takes seconds, so rather than creating one per test method we
     * start a single instance lazily and {@linkplain #dropAllTables(MySQLContainer) wipe its
     * schema} before each test to keep the tests isolated from one another.
     */
    private static @Nullable MySQLContainer serverContainer = null;

    /**
     * The factory handed to the previous test.
     *
     * <p>JUnit creates a fresh test instance per method, so the factory cannot be tracked in an
     * instance field. We keep it here and close its connection pool when the next test asks for a
     * factory — by that point the previous test has already been torn down.
     */
    private static @Nullable JdbcStorageFactory previousFactory = null;

    /**
     * Controls whether ALL tests in this package are enabled.
     *
     * <p>The MySQL-based tests need a running Docker daemon to start the database container.
     * They are therefore enabled only when Docker is available, and are skipped otherwise so
     * that the build stays green in environments without Docker (e.g. local machines or CI
     * agents that do not provide Docker).
     */
    static final boolean enabled = DockerClientFactory.instance()
                                                      .isDockerAvailable();

    /**
     * Prevents instantiation of this utility.
     */
    private MysqlTests() {
        // Do nothing.
    }

    /**
     * Prepares a storage factory connected to the shared MySQL container for the next test.
     *
     * <p>Closes the connection pool used by the previous test, wipes the database schema so that
     * the test starts with a clean slate, and returns a freshly built factory.
     */
    public static synchronized JdbcStorageFactory newFactory() {
        closePreviousFactory();
        var container = sharedContainer();
        dropAllTables(container);
        var factory = JdbcStorageFactory.newBuilder()
                .setDataSource(wrap(container))
                .setTypeMapping(mysqlMapping())
                .build();
        previousFactory = factory;
        return factory;
    }

    /**
     * Returns the MySQL container shared by all the tests, starting it on the first call.
     */
    private static synchronized MySQLContainer sharedContainer() {
        if (serverContainer == null) {
            var container = new MySQLContainer(IMAGE);
            // Allow the `caching_sha2_password` handshake over a non-TLS test connection.
            container.withUrlParam("allowPublicKeyRetrieval", "true");
            // Reuse the container across Gradle runs when the user opts in via
            // `~/.testcontainers.properties` (`testcontainers.reuse.enable=true`).
            // The flag is ignored when reuse is not enabled, so this is safe by default.
            container.withReuse(true);
            container.start();
            serverContainer = container;
        }
        return serverContainer;
    }

    /**
     * Closes the connection pool used by the previous test, if any.
     */
    private static void closePreviousFactory() {
        if (previousFactory != null && previousFactory.isOpen()) {
            previousFactory.close();
        }
        previousFactory = null;
    }

    /**
     * Removes all the tables from the container's database, giving each test a clean schema.
     */
    @SuppressWarnings("JDBCExecuteWithNonConstantString" /* safe because the query is
                parameterized by a table name */)
    private static void dropAllTables(MySQLContainer container) {
        try (var connection = DriverManager.getConnection(container.getJdbcUrl(),
                                                          container.getUsername(),
                                                          container.getPassword());
             var statement = connection.createStatement()
        ) {
            var tables = collectTableNames(statement);
            statement.execute("SET FOREIGN_KEY_CHECKS = 0");
            for (var table : tables) {
                statement.execute("DROP TABLE IF EXISTS `" + table + '`');
            }
            statement.execute("SET FOREIGN_KEY_CHECKS = 1");
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to reset the MySQL test schema.", e);
        }
    }

    private static ArrayList<String> collectTableNames(Statement statement) throws
                                                                            SQLException {
        var tables = new ArrayList<String>();
        try (var tableNames = statement.executeQuery("SHOW TABLES")) {
            while (tableNames.next()) {
                tables.add(tableNames.getString(1));
            }
        }
        return tables;
    }

    /**
     * Creates a new {@code DataSourceWrapper} around the passed instance of MySQL container.
     *
     * <p>The connections are pooled via HikariCP, with its default settings.
     */
    private static DataSourceWrapper wrap(MySQLContainer container) {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());

        var hikariSource = new HikariDataSource(config);
        var dataSource = DataSourceWrapper.wrap(hikariSource);
        return dataSource;
    }

    /**
     * Returns a type mapping compatible with the MySQL version
     * run via the {@linkplain #sharedContainer() container}.
     */
    private static PredefinedMapping mysqlMapping() {
        return MYSQL_9_7;
    }
}
