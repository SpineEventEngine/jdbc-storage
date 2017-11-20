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

import com.google.protobuf.StringValue;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.Snapshot;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.validate.StringValueVBuilder;
import org.junit.Test;

import static com.querydsl.core.types.ExpressionUtils.path;
import static io.spine.Identifier.newUuid;
import static io.spine.core.Versions.newVersion;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.GivenDataSource.withoutSuperpowers;
import static io.spine.server.storage.jdbc.StandardMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.KIND;
import static io.spine.time.Time.getCurrentTime;
import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Grankin
 */
public class AggregateEventRecordTableShould {

    @Test(expected = IllegalStateException.class)
    public void throw_on_attempt_to_update_event_record() {
        final AggregateEventRecordTable<String> table =
                new AggregateEventRecordTable<>(AnAggregate.class, withoutSuperpowers(), MYSQL_5_7);
        table.update(newUuid(), AggregateEventRecord.getDefaultInstance());
    }

    @Test
    public void store_record_kind_in_string_representation() {
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

    private static class AnAggregate extends Aggregate<String, StringValue, StringValueVBuilder> {
        private AnAggregate(String id) {
            super(id);
        }
    }
}
