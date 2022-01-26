/*
 * Copyright 2021, TeamDev. All rights reserved.
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
import com.querydsl.sql.SQLTemplatesRegistry;
import io.spine.annotation.Internal;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Wrapper for {@link DataSource} instances.
 */
@Internal
public interface DataSourceWrapper extends AutoCloseable {

    /**
     * Retrieves a wrapped connection with the given auto commit mode.
     *
     * @throws DatabaseException
     *         if an error occurs during an interaction with the DB
     * @throws IllegalStateException
     *         if the data source is closed
     * @see Connection#setAutoCommit(boolean)
     */
    ConnectionWrapper getConnection(boolean autoCommit) throws DatabaseException;

    /**
     * Obtains the metadata of the wrapped {@link DataSource}.
     *
     * @throws DatabaseException
     *         if an error occurs during an interaction with the DB
     * @throws IllegalStateException
     *         if the data source is closed
     */
    DataSourceMetaData metaData() throws DatabaseException;

    /**
     * Obtains {@linkplain SQLTemplates templates} for the JDBC dialect.
     *
     * @return templates for a particular JDBC implementation
     */
    default SQLTemplates templates() {
        try (var connection = getConnection(true)) {
            var metaData = connection.get()
                                     .getMetaData();
            var templatesRegistry = new SQLTemplatesRegistry();
            return templatesRegistry.getTemplates(metaData);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Closes the wrapped {@link DataSource}.
     *
     * <p>Overridden from {@link AutoCloseable} to get rid of thrown exception type.
     *
     * @throws DatabaseException
     *         if an error occurs during an interaction with the DB
     * @throws IllegalStateException
     *         if the data source is closed
     */
    @Override
    void close();

    /**
     * Returns {@code true} if the data source is closed, {@code false} otherwise.
     */
    boolean isClosed();

    /** Wraps a {@link DataSource} implementation. */
    static DataSourceWrapper wrap(DataSource dataSource) {
        return new DefaultDataSourceWrapper(dataSource);
    }
}
