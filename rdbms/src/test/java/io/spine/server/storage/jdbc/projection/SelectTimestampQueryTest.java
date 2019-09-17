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

package io.spine.server.storage.jdbc.projection;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("SelectTimestampQuery should")
class SelectTimestampQueryTest {

    @Test
    @DisplayName("return `null` if `seconds` and `nanos` fields are not set")
    void returnNullForEmptyTimestamp() throws SQLException {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());

        LastHandledEventTimeTable table = new LastHandledEventTimeTable(dataSource, MYSQL_5_7);
        table.create();
        String tableName = table.name();

        String id = newUuid();
        table.composeInsertQuery(id, Timestamp.getDefaultInstance())
             .execute();

        SelectTimestampQuery query = SelectTimestampQuery.newBuilder()
                                                         .setTableName(tableName)
                                                         .setDataSource(dataSource)
                                                         .setId(id)
                                                         .setIdColumn(table.idColumn())
                                                         .build();
        ResultSet resultSet = query.query()
                                   .getResults();
        assertThat(resultSet.next())
                .isTrue();

        Message deserialized = query.readMessage(resultSet);
        assertNull(deserialized);
    }
}
