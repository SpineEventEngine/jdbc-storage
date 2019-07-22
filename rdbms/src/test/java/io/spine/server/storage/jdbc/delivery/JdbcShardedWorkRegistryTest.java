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

package io.spine.server.storage.jdbc.delivery;

import com.google.common.testing.NullPointerTester;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.base.Identifier;
import io.spine.server.NodeId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.message.JdbcMessageStorageTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Time.currentTime;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.delivery.given.TestShardIndex.newIndex;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@DisplayName("JdbcShardedWorkRegistry should")
class JdbcShardedWorkRegistryTest extends JdbcMessageStorageTest<ShardIndex,
                                                                 ShardSessionRecord,
                                                                 ShardSessionReadRequest,
                                                                 JdbcShardedWorkRegistry> {

    private static final ShardIndex index = newIndex(1, 15);

    private static final NodeId nodeId = newNode();

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .setDefault(NodeId.class, newNode())
                .setDefault(ShardIndex.class, newIndex(4, 5))
                .setDefault(ShardSessionRecord.class, ShardSessionRecord.getDefaultInstance())
                .setDefault(ShardSessionReadRequest.class,
                            new ShardSessionReadRequest(newIndex(6, 10)))
                .testAllPublicInstanceMethods(storage());
    }

    @Test
    @DisplayName("pick up the shard and write a corresponding record to the storage")
    void pickUp() {
        Optional<ShardProcessingSession> session = storage().pickUp(index, nodeId);
        assertTrue(session.isPresent());
        assertThat(session.get()
                          .shardIndex()).isEqualTo(index);

        ShardSessionRecord record = readSingleRecord(index);
        assertThat(record.getIndex()).isEqualTo(index);
        assertThat(record.getPickedBy()).isEqualTo(nodeId);
    }

    @Test
    @DisplayName("not be able to pick up the shard if it's already picked up")
    void cannotPickUpIfTaken() {

        Optional<ShardProcessingSession> session = storage().pickUp(index, nodeId);
        assertTrue(session.isPresent());

        Optional<ShardProcessingSession> sameIdxSameNode = storage().pickUp(index, nodeId);
        assertFalse(sameIdxSameNode.isPresent());

        Optional<ShardProcessingSession> sameIdxAnotherNode = storage().pickUp(index, newNode());
        assertFalse(sameIdxAnotherNode.isPresent());

        ShardIndex anotherIdx = newIndex(24, 100);
        Optional<ShardProcessingSession> anotherIdxSameNode = storage().pickUp(anotherIdx, nodeId);
        assertTrue(anotherIdxSameNode.isPresent());

        Optional<ShardProcessingSession> anotherIdxAnotherNode =
                storage().pickUp(anotherIdx, newNode());
        assertFalse(anotherIdxAnotherNode.isPresent());
    }

    @Test
    @DisplayName("complete the shard session (once picked up) and make it available for picking up")
    void completeSessionAndMakeItAvailable() {
        Optional<ShardProcessingSession> optional = storage().pickUp(index, nodeId);
        assertTrue(optional.isPresent());

        Timestamp whenPickedFirst = readSingleRecord(index).getWhenLastPicked();

        JdbcShardProcessingSession session = (JdbcShardProcessingSession) optional.get();
        session.complete();

        ShardSessionRecord completedRecord = readSingleRecord(index);
        assertFalse(completedRecord.hasPickedBy());

        NodeId anotherNode = newNode();
        Optional<ShardProcessingSession> anotherOptional = storage().pickUp(index, anotherNode);
        assertTrue(anotherOptional.isPresent());

        ShardSessionRecord secondSessionRecord = readSingleRecord(index);
        assertThat(secondSessionRecord.getPickedBy()).isEqualTo(anotherNode);

        Timestamp whenPickedSecond = secondSessionRecord.getWhenLastPicked();
        assertTrue(Timestamps.compare(whenPickedFirst, whenPickedSecond) < 0);
    }

    @Override
    protected JdbcShardedWorkRegistry newStorage(Class<? extends Entity<?, ?>> aClass) {
        DataSourceWrapper dataSource = whichIsStoredInMemory("jdbcShardedWorkRegistryTest");
        JdbcShardedWorkRegistry registry = JdbcShardedWorkRegistry
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(MYSQL_5_7)
                .build();
        return registry;
    }

    @Override
    protected ShardSessionRecord newStorageRecord() {
        ShardIndex index = newIndex();
        NodeId node = newNode();
        ShardSessionRecord record = ShardSessionRecord
                .newBuilder()
                .setIndex(index)
                .setPickedBy(node)
                .setWhenLastPicked(currentTime())
                .build();
        return record;
    }

    @Override
    protected ShardIndex newId() {
        return newIndex();
    }

    @Override
    protected ShardSessionReadRequest newReadRequest(ShardIndex shardIndex) {
        return new ShardSessionReadRequest(shardIndex);
    }

    private static NodeId newNode() {
        return NodeId.newBuilder()
                     .setValue(Identifier.newUuid())
                     .vBuild();
    }

    private ShardSessionRecord readSingleRecord(ShardIndex index) {
        Iterator<ShardSessionRecord> records = storage().readByIndex(index);
        assertTrue(records.hasNext());

        return records.next();
    }
}
