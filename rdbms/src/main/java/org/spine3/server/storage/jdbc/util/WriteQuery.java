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

package org.spine3.server.storage.jdbc.util;

import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A query which is executed in order to write to the data source.
 *
 * @author Alexander Litus
 */
@Internal
public abstract class WriteQuery {

    private final DataSourceWrapper dataSource;

    protected WriteQuery(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Executes a write query.
     */
    public void execute() {
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            try (PreparedStatement statement = prepareStatement(connection)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                logError(e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * Prepares SQL statement using the connection.
     */
    protected abstract PreparedStatement prepareStatement(ConnectionWrapper connection);

    /**
     * Logs an occurred exception.
     */
    protected abstract void logError(SQLException exception);
}
