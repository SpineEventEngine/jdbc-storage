/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

/**
 * The implementation base for the queries to an SQL database.
 *
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
abstract class StorageQuery {

    private final String query;
    private final DataSourceWrapper dataSource;
    private final Logger logger;

    protected StorageQuery(Builder<? extends Builder, ? extends StorageQuery> builder) {
        this.query = builder.query;
        this.dataSource = builder.dataSource;
        this.logger = builder.logger;
    }

    /**
     * Prepares a statement using {@linkplain #getQueryParameters() parameters}.
     *
     * @param connection the connection to use
     * @return a {@link PreparedStatement}, which is ready to use
     */
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = connection.prepareStatement(query);
        final Set<Integer> parameterIds = getQueryParameters().getIdentifiers();
        try {
            for (Integer parameterId : parameterIds) {
                final Object parameterValue = getQueryParameters().getValue(parameterId);
                statement.setObject(parameterId, parameterValue);
            }
            return statement;
        } catch (SQLException e) {
            getLogger().error("Failed to prepare statement ", e);
            throw new DatabaseException(e);
        }
    }

    /**
     * Obtains {@linkplain Parameters parameters} to set during
     * {@linkplain #prepareStatement(ConnectionWrapper) statement preparation}.
     *
     * @return parameters for this query
     */
    protected abstract Parameters getQueryParameters();

    public String getQuery() {
        return query;
    }

    protected Logger getLogger() {
        return logger;
    }

    protected ConnectionWrapper getConnection(boolean autocommit) {
        return dataSource.getConnection(autocommit);
    }

    abstract static class Builder<B extends Builder<B, Q>, Q extends StorageQuery> {

        private String query;
        private DataSourceWrapper dataSource;
        private Logger logger;

        public abstract Q build();

        protected abstract B getThis();

        public B setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return getThis();
        }

        public B setQuery(String query) {
            this.query = query;
            return getThis();
        }

        public B setLogger(Logger logger) {
            this.logger = logger;
            return getThis();
        }

        public DataSourceWrapper getDataSource() {
            return dataSource;
        }
    }
}
