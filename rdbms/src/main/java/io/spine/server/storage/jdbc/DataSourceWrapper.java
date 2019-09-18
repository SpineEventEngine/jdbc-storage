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

import io.spine.annotation.Internal;

import javax.sql.DataSource;
import java.sql.Connection;

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
     * Obtains the meta data of the wrapped {@link DataSource}.
     *
     * @throws DatabaseException
     *         if an error occurs during an interaction with the DB
     * @throws IllegalStateException
     *         if the data source is closed
     */
    DataSourceMetaData metaData() throws DatabaseException;

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
