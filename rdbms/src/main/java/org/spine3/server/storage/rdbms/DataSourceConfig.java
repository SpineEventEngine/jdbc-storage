/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.rdbms;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * The configuration object for an underlying {@link DataSource} or {@link Driver} implementation and a connection pool.
 *
 * <p><b>NOTE:</b> the <a href="https://github.com/brettwooldridge/HikariCP">HikariCP</a> connection pool
 * comes with <b>sane</b> defaults that perform well in most deployments without additional tweaking.
 * Every property is optional unless it is marked as required.
 *
 * @see Builder
 * @author Alexander Litus
 */
public class DataSourceConfig {

    /**
     * Required
     */
    private final String dataSourceClassName;
    private final String jdbcUrl;
    private final String username;
    private final String password;

    /**
     * Optional
     */
    private final Boolean autoCommit;
    private final Long connectionTimeout;
    private final Long idleTimeout;
    private final Long maxLifetime;
    private final String connectionTestQuery;
    private final Integer maxPoolSize;
    private final String poolName;

    private DataSourceConfig(Builder builder) {
        this.dataSourceClassName = builder.getDataSourceClassName();
        this.jdbcUrl = builder.getJdbcUrl();
        this.username = builder.getUsername();
        this.password = builder.getPassword();

        this.autoCommit = builder.isAutoCommit();
        this.connectionTimeout = builder.getConnectionTimeout();
        this.idleTimeout = builder.getIdleTimeout();
        this.maxLifetime = builder.getMaxLifetime();
        this.connectionTestQuery = builder.getConnectionTestQuery();
        this.maxPoolSize = builder.getMaxPoolSize();
        this.poolName = builder.getPoolName();
    }

    /**
     * Creates a new builder for {@code DataSourceConfig}.
     *
     * @return a new builder instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * See {@link Builder#setDataSourceClassName(String)}.
     */
    public String getDataSourceClassName() {
        return dataSourceClassName;
    }

    /**
     * See {@link Builder#setJdbcUrl(String)}.
     */
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    /**
     * See {@link Builder#setUsername(String)}.
     */
    public String getUsername() {
        return username;
    }

    /**
     * See {@link Builder#setPassword(String)}.
     */
    public String getPassword() {
        return password;
    }

    /**
     * See {@link Builder#setAutoCommit(Boolean)}.
     */
    public Boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * See {@link Builder#setConnectionTimeout(Long)}.
     */
    public Long getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * See {@link Builder#setIdleTimeout(Long)}.
     */
    public Long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * See {@link Builder#setMaxLifetime(Long)}.
     */
    public Long getMaxLifetime() {
        return maxLifetime;
    }

    /**
     * See {@link Builder#setConnectionTestQuery(String)}.
     */
    public String getConnectionTestQuery() {
        return connectionTestQuery;
    }

    /**
     * See {@link Builder#setMaxPoolSize(Integer)}.
     */
    public Integer getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * See {@link Builder#setPoolName(String)}.
     */
    public String getPoolName() {
        return poolName;
    }

    /**
     * The builder for {@link DataSourceConfig}.
     */
    public static class Builder {

        /**
         * Required
         */
        private String dataSourceClassName;
        private String jdbcUrl;
        private String username;
        private String password;

        /**
         * Optional
         */
        private Boolean autoCommit;
        private Long connectionTimeout;
        private Long idleTimeout;
        private Long maxLifetime;
        private String connectionTestQuery;
        private Integer maxPoolSize;
        private String poolName;

        @SuppressWarnings("MethodWithMoreThanThreeNegations")
        public DataSourceConfig build() {
            // Either dataSourceClassName or jdbcUrl is required
            if (dataSourceClassName == null) {
                checkState(!isNullOrEmpty(jdbcUrl), "jdbcUrl");
            }
            if (jdbcUrl == null) {
                checkState(!isNullOrEmpty(dataSourceClassName), "dataSourceClassName");
            }

            // Required, but can be empty
            checkNotNull(username, "username");
            checkNotNull(password, "password");

            // Optional
            if (connectionTestQuery != null) {
                checkState(!connectionTestQuery.isEmpty(), "connectionTestQuery");
            }
            if (poolName != null) {
                checkState(!poolName.isEmpty(), "poolName");
            }

            final DataSourceConfig config = new DataSourceConfig(this);
            return config;
        }

        /**
         * See {@link #setDataSourceClassName(String)}.
         */
        public String getDataSourceClassName() {
            return dataSourceClassName;
        }

        /**
         * Sets the name of the class implementing {@link DataSource} provided by the JDBC driver.
         *
         * <p>This property is <b>required</b> if {@code jdbcUrl} is not set.
         *
         * <p>Consult the documentation for your specific JDBC driver to get this class name,
         * or see <a href="https://github.com/brettwooldridge/HikariCP#popular-datasource-class-names">popular classes</a>.
         *
         * <p><b>NOTE:</b> XA data sources are not supported.
         * XA requires a real transaction manager like <a href="https://github.com/bitronix/btm">bitronix</a>.
         *
         * <p><b>NOTE:</b> this property may not be required if you are using {@code jdbcUrl}
         * for "old-school" DriverManager-based JDBC driver configuration (see {@link #setJdbcUrl(String)}).
         *
         * <p>Default: none
         *
         * @param dataSourceClassName the class name to set
         */
        public Builder setDataSourceClassName(String dataSourceClassName) {
            this.dataSourceClassName = dataSourceClassName;
            return this;
        }

        /**
         * See {@link #setJdbcUrl(String)}.
         */
        public String getJdbcUrl() {
            return jdbcUrl;
        }

        /**
         * This property directs the connection pool to use {@link DriverManager}-based configuration.
         *
         * <p>This property is <b>required</b> if {@code dataSourceClassName} is not set.
         *
         * <p>The {@link DataSource}-based configuration (see {@link #setDataSourceClassName(String)})
         * is superior for a variety of reasons, but for many deployments there is little significant difference.
         *
         * <p>When using this property with "old" drivers, you may also need to set the {@code driverClassName} property,
         * but try it first without.
         *
         * <p><b>NOTE:</b> if this property is used, you may still use {@link DataSource} properties to configure your driver
         * and is in fact recommended over driver parameters specified in the URL itself.
         *
         * <p>Examples of JDBC URL (HyperSQL DB):
         *
         * <p>{@code jdbc:hsqldb:hsql://localhost:9001/dbname;ifexists=true}
         * <p>{@code jdbc:hsqldb:mem:inmemorydb} (for in-memory database)
         *
         * <p>Default: none
         *
         * @param jdbcUrl a database url of the form {@code jdbc:subprotocol:subname}
         */
        public Builder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        /**
         * See {@link #setUsername(String)}.
         */
        public String getUsername() {
            return username;
        }

        /**
         * This property sets the default authentication username used for obtaining Connections from the underlying driver.
         *
         * <p>This property is <b>required</b>.
         *
         * <p>Note that for {@link DataSource}-based configuration this works in a very deterministic fashion by calling
         * {@link DataSource#getConnection(String, String)}.
         *
         * <p>However, for {@link Driver}-based configurations, every driver is different.
         * In this case, the username property is set in the {@link Properties}
         * passed to {@link DriverManager#getConnection(String, Properties)} method.
         *
         * <p>Default: none
         *
         * @param username the username to set
         */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * See {@link #setPassword(String)}.
         */
        public String getPassword() {
            return password;
        }

        /**
         * Sets the default authentication password used for obtaining Connections from the underlying driver.
         *
         * <p>This property is <b>required</b>.
         *
         * <p>Note that for {@link DataSource}-based configuration this works in a very deterministic fashion by calling
         * {@link DataSource#getConnection(String, String)}.
         *
         * <p>However, for {@link Driver}-based configurations, every driver is different.
         * In this case, the password property is set in the {@link Properties}
         * passed to {@link DriverManager#getConnection(String, Properties)} method.
         *
         * <p>Default: none
         *
         * @param password the password to set
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * See {@link #setAutoCommit(Boolean)}.
         */
        public Boolean isAutoCommit() {
            return autoCommit;
        }

        /**
         * Sets the default auto-commit behavior of all connections.
         *
         * <p>Default: {@code true}
         *
         * @see Connection#setAutoCommit(boolean)
         * @param autoCommit the value to set
         */
        public Builder setAutoCommit(Boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        /**
         * See {@link #setConnectionTimeout(Long)}.
         */
        public Long getConnectionTimeout() {
            return connectionTimeout;
        }

        /**
         * Sets the maximum number of milliseconds to wait for a connection from the pool.
         * If this time is exceeded without a connection becoming available, the {@link SQLException} will be thrown.
         *
         * <p>1000 ms is the minimum value.
         * <p>If a value of zero is passed, then {@link Integer#MAX_VALUE} is set instead.
         *
         * <p>Default: 30 000 (30 seconds)
         *
         * @param connectionTimeout the number of milliseconds to set
         */
        public Builder setConnectionTimeout(Long connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * See {@link #setIdleTimeout(Long)}.
         */
        public Long getIdleTimeout() {
            return idleTimeout;
        }

        /**
         * Sets the maximum amount of time that a connection is allowed to sit idle in the pool.
         * Whether a connection is retired as idle or not is subject to a maximum variation of +30 seconds,
         * and average variation of +15 seconds.
         *
         * <p>A connection will never be retired as idle before this timeout.
         *
         * <p>A value of 0 means that idle connections are never removed from the pool.
         *
         * <p>Default: 600 000 (10 minutes)
         *
         * @param idleTimeout the timeout in milliseconds
         */
        public Builder setIdleTimeout(Long idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }

        /**
         * See {@link #setMaxLifetime(Long)}.
         */
        public Long getMaxLifetime() {
            return maxLifetime;
        }

        /**
         * Sets the maximum lifetime of a connection in the pool.
         *
         * <p>When a connection reaches this timeout it is retired from the pool,
         * subject to a maximum variation of +30 seconds.
         *
         * <p>An in-use connection is never retired, only when it is closed will it is removed then.
         *
         * <b>NOTE:</b> It is strongly recommended to set this value,
         * and it should be at least 30 seconds less than any database-level connection timeout.
         *
         * <p>A value of 0 indicates no maximum lifetime (infinite lifetime), subject of course to the {@code idleTimeout} setting.
         *
         * <p>Default: 1 800 000 (30 minutes)
         *
         * @param maxLifetime the number of milliseconds to set
         */
        public Builder setMaxLifetime(Long maxLifetime) {
            this.maxLifetime = maxLifetime;
            return this;
        }

        /**
         * See {@link #setConnectionTestQuery(String)}.
         */
        public String getConnectionTestQuery() {
            return connectionTestQuery;
        }

        /**
         * If your driver supports JDBC4 it is strongly recommended <b>not setting</b> this property.
         * It is for "legacy" databases that do not support the JDBC4 {@link Connection#isValid(int)} API.
         *
         * <p>It is the query that will be executed just before a connection is given to you from the pool
         * to validate that the connection to the database is still alive.
         *
         * <p>Again, try running the pool without this property, an error will be logged if your driver is not JDBC4 compliant.
         *
         * <p>Default: none
         *
         * @param connectionTestQuery the query to set
         */
        public Builder setConnectionTestQuery(String connectionTestQuery) {
            this.connectionTestQuery = connectionTestQuery;
            return this;
        }

        /**
         * See {@link #setMaxPoolSize(Integer)}.
         */
        public Integer getMaxPoolSize() {
            return maxPoolSize;
        }

        /**
         * Sets the maximum size that the pool is allowed to reach, including both idle and in-use connections.
         *
         * <p>Basically this value will determine the maximum number of actual connections to the database backend.
         *
         * <p>A reasonable value for this is best determined by your execution environment.
         *
         * <p>When the pool reaches this size, and no idle connections are available,
         * calls to getConnection() will block for up to {@code connectionTimeout} milliseconds before timing out.
         *
         * <p>Default: 10
         *
         * @see #setConnectionTimeout(Long)
         * @param maxPoolSize the value to set
         */
        public Builder setMaxPoolSize(Integer maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }
        /**
         * See {@link #setPoolName(String)}.
         */
        public String getPoolName() {
            return poolName;
        }

        /**
         * Sets a name of the connection pool and appears mainly in logging and JMX management consoles
         * to identify pools and pool configurations.
         *
         * <p>Default: auto-generated
         *
         * @param poolName the name to set
         */
        public Builder setPoolName(String poolName) {
            this.poolName = poolName;
            return this;
        }
    }
}
