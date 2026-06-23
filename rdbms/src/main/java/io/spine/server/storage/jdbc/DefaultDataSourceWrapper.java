/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import com.querydsl.sql.SQLTemplates;
import io.spine.logging.WithLogging;

import javax.sql.DataSource;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * A default implementation of the {@link DataSourceWrapper}.
 */
final class DefaultDataSourceWrapper implements DataSourceWrapper, WithLogging {

    private final DataSource dataSource;
    private final SQLTemplates templates;
    private boolean isClosed;

    DefaultDataSourceWrapper(DataSource dataSource) {
        this.dataSource = dataSource;
        this.templates = DataSourceWrapper.super.templates();
    }

    @Override
    public ConnectionWrapper getConnection(boolean autoCommit) {
        checkNotClosed();
        try {
            var connection = dataSource.getConnection();
            connection.setAutoCommit(autoCommit);
            var wrapper = ConnectionWrapper.wrap(connection);
            return wrapper;
        } catch (SQLException e) {
            logger().atError()
                    .withCause(e)
                    .log(() -> "Failed to obtain a connection.");
            throw new DatabaseException(e);
        }
    }

    @Override
    public DataSourceMetaData metaData() {
        checkNotClosed();
        try (final var connection = getConnection(true)) {
            var metaData = connection.get()
                                     .getMetaData();
            return DataSourceMetaData.of(metaData);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public SQLTemplates templates() {
        return templates;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Closes wrapped {@link DataSource} implementation if it implements {@link AutoCloseable}.
     *
     * <p>Otherwise a warning is logged.
     */
    @Override
    public void close() {
        checkNotClosed();
        isClosed = true;
        if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                logger().atError()
                        .withCause(e)
                        .log(() -> "Error occurred while closing DataSource.");
                throw new DatabaseException(e);
            }
            return;
        }
        var dataSourceCls = dataSource.getClass();
        logger().atWarning()
                .log(() -> format("Close method is not implemented in `%s`.",
                                  dataSourceCls.getCanonicalName()));
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private void checkNotClosed() {
        checkState(!isClosed(), "The data source is closed.");
    }
}
