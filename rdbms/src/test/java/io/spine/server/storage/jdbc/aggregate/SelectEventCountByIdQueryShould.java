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

package io.spine.server.storage.jdbc.aggregate;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.aggregate.SelectEventCountByIdQuery.newBuilder;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * @author Dmytro Grankin
 */
public class SelectEventCountByIdQueryShould {

    @Test
    public void return_null_if_event_count_is_null() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        final SelectEventCountByIdQuery<Object> query = newBuilder().setTableName(newUuid())
                                                                    .setDataSource(dataSource)
                                                                    .build();
        final Message deserialized = query.readMessage(resultSet);
        assertNull(deserialized);
    }
}
