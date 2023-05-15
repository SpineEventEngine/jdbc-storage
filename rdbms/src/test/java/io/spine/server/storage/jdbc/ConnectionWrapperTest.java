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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("ConnectionWrapper should")
class ConnectionWrapperTest {

    private final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());

    @Test
    @DisplayName("store and retrieve connection")
    void storeAndRetrieveConnection() {
        Connection connection = connection();
        ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        Connection stored = wrapper.get();
        assertThat(stored)
                .isSameInstanceAs(connection);
    }

    @Test
    @DisplayName("throw `DatabaseException` in case of `SQLException` on commit")
    void handleExceptionOnCommit() throws SQLException {
        Connection connection = connection();
        // Close the connection so it can't commit.
        connection.close();
        ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        assertThrows(DatabaseException.class, wrapper::commit);
    }

    @Test
    @DisplayName("throw `DatabaseException` in case of `SQLException` on rollback")
    void handleExceptionOnRollback() throws SQLException {
        Connection connection = connection();
        // Close the connection so the rollback is not available.
        connection.close();
        ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        assertThrows(DatabaseException.class, wrapper::rollback);
    }

    @Test
    @DisplayName("throw `DatabaseException` in case of `SQLException` on preparing statement")
    void handleExceptionOnPrepareStatement() throws SQLException {
        Connection connection = connection();
        // Close the connection so the statement cannot be prepared.
        connection.close();
        ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        assertThrows(DatabaseException.class,
                     () -> wrapper.prepareStatement("SOME SQL STATEMENT."));
    }

    private Connection connection() {
        ConnectionWrapper connection = dataSource.getConnection(false);
        return connection.get();
    }
}
