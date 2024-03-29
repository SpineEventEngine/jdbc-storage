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

package io.spine.server.storage.jdbc.query;

import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.AbstractQuery.TransactionHandler;
import io.spine.server.storage.jdbc.query.given.Given.AStorageQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.query.given.Given.storageQueryBuilder;
import static io.spine.server.storage.jdbc.query.given.Given.tableSpec;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;

@DisplayName("`AbstractQuery` should")
class AbstractQueryTest {

    private final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
    private final AStorageQuery query = storageQueryBuilder().setTableSpec(tableSpec())
                                                             .setDataSource(dataSource)
                                                             .build();

    /**
     * A commit is executed after a query execution, but a {@code ResultSet} should be used after
     * this, hence the result set should not be closed. This test verifies that the correct
     * holdability was set, assuming that the JDBC implementation does the rest correctly.
     */
    @Test
    @DisplayName("set `HOLD_CURSORS_OVER_COMMIT` for connection")
    void holdCursorsOverCommit() throws SQLException {
        var connection = query.factory()
                              .getConnection();

        assertThat(connection.getHoldability())
                .isEqualTo(HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    @DisplayName("specify a `TransactionHandler` as one of the transaction listeners")
    void injectTransactionHandler() {
        var configuration = query.factory()
                                 .getConfiguration();
        var listeners = configuration.getListeners();
        assertThat(listeners.getListeners())
                .contains(TransactionHandler.INSTANCE);
    }

    @Test
    @DisplayName("be able to provide a MySQL-specific query factory")
    void returnMySqlSpecificFactory() {
        var factory = query.mySqlFactory();
        assertThat(factory).isNotNull();
    }
}
