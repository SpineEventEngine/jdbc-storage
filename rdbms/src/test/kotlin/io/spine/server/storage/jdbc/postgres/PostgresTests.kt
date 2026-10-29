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

package io.spine.server.storage.jdbc.postgres

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.spine.server.storage.jdbc.DataSourceWrapper
import io.spine.server.storage.jdbc.JdbcStorageFactory
import io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_18
import java.sql.DriverManager
import org.testcontainers.postgresql.PostgreSQLContainer

/**
 * Defines the common routines to use in tests against a PostgreSQL instance.
 *
 * The suites that use it should be annotated with
 * `@Testcontainers(disabledWithoutDocker = true)`, so that they are skipped
 * in environments without Docker.
 */
internal object PostgresTests {

    /**
     * The Docker image of the PostgreSQL server used in the tests.
     *
     * It is the major release the [mapping][POSTGRESQL_18] is made for;
     * `PostgresServerVersionSpec` checks that the two match. The tag follows the patch
     * releases of that major one. The image is published for `arm64` in addition to `amd64`,
     * so the container boots natively on Apple Silicon.
     *
     * Every PostgreSQL test of this module uses this image.
     */
    const val IMAGE = "postgres:18-alpine"

    /**
     * A single PostgreSQL container shared by all the tests in this package.
     *
     * Booting a container takes seconds, so rather than creating one per test method we
     * start a single instance lazily and [wipe its schema][dropAllTables] before each test
     * to keep the tests isolated from one another.
     */
    private val container: PostgreSQLContainer by lazy {
        PostgreSQLContainer(IMAGE).apply {
            // Reuse the container across Gradle runs when the user opts in via
            // `~/.testcontainers.properties` (`testcontainers.reuse.enable=true`).
            // The flag is ignored when reuse is not enabled, so this is safe by default.
            withReuse(true)
            start()
        }
    }

    /**
     * The factory handed to the previous test.
     *
     * We close its connection pool when the next test asks for a factory — by that point
     * the previous test has already been torn down.
     */
    private var previousFactory: JdbcStorageFactory? = null

    /**
     * Prepares a storage factory connected to the shared PostgreSQL container for the next test.
     *
     * Closes the connection pool used by the previous test, wipes the database schema so that
     * the test starts with a clean slate, and returns a freshly built factory.
     */
    @Synchronized
    fun newFactory(): JdbcStorageFactory {
        closePreviousFactory()
        dropAllTables()
        val factory = JdbcStorageFactory.newBuilder()
            .setDataSource(dataSource())
            .setTypeMapping(POSTGRESQL_18)
            .build()
        previousFactory = factory
        return factory
    }

    private fun closePreviousFactory() {
        previousFactory?.takeIf { it.isOpen }?.close()
        previousFactory = null
    }

    /**
     * Removes all the tables from the container's database by recreating its `public` schema.
     */
    private fun dropAllTables() {
        DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
            .use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("DROP SCHEMA public CASCADE")
                    statement.execute("CREATE SCHEMA public")
                }
            }
    }

    /**
     * Creates a data source pooling the connections to the container via HikariCP,
     * with its default settings.
     */
    private fun dataSource(): DataSourceWrapper {
        val config = HikariConfig().apply {
            jdbcUrl = container.jdbcUrl
            username = container.username
            password = container.password
        }
        return DataSourceWrapper.wrap(HikariDataSource(config))
    }
}
