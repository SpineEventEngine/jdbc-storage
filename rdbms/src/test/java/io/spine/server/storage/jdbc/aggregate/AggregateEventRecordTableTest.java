/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.Snapshot;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.aggregate.given.AggregateEventRecordTableTestEnv.AnAggregate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.querydsl.core.types.ExpressionUtils.path;
import static io.spine.base.Identifier.newUuid;
import static io.spine.base.Time.getCurrentTime;
import static io.spine.core.Versions.newVersion;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.GivenDataSource.withoutSuperpowers;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.KIND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Dmytro Grankin
 */
@DisplayName("AggregateEventRecordTable should")
class AggregateEventRecordTableTest {

    @Test
    @DisplayName("throw ISE on attempt to update event record")
    void throwOnUpdateEventRecord() {
        final AggregateEventRecordTable<String> table =
                new AggregateEventRecordTable<>(AnAggregate.class, withoutSuperpowers(), MYSQL_5_7);
        assertThrows(IllegalStateException.class,
                     () -> table.update(newUuid(), AggregateEventRecord.getDefaultInstance()));
    }

    @Test
    @DisplayName("store record kind in string representation")
    void storeRecordKind() {
        final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        final AggregateEventRecordTable<String> table =
                new AggregateEventRecordTable<>(AnAggregate.class, dataSource, MYSQL_5_7);
        table.create();

        final Snapshot snapshot = Snapshot.newBuilder()
                                          .setVersion(newVersion(5, getCurrentTime()))
                                          .build();
        final AggregateEventRecord record = AggregateEventRecord.newBuilder()
                                                                .setSnapshot(snapshot)
                                                                .build();
        final InsertAggregateRecordQuery<String> query = table.composeInsertQuery(newUuid(),
                                                                                  record);
        query.execute();

        final String expectedKind = record.getKindCase()
                                          .toString();
        final String actualKind = query.factory()
                                       .select(path(String.class, KIND.name()))
                                       .from(query.table())
                                       .fetchFirst();
        assertEquals(expectedKind, actualKind);
    }
}
