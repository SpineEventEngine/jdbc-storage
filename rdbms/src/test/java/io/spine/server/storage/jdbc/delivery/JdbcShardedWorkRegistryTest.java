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

package io.spine.server.storage.jdbc.delivery;

import com.google.common.testing.NullPointerTester;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.base.Identifier;
import io.spine.server.NodeId;
import io.spine.server.delivery.PickUpOutcome;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.delivery.ShardedWorkRegistryTest;
import io.spine.server.delivery.WorkerId;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.server.storage.jdbc.delivery.given.TestShardIndex.newIndex;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;

@DisplayName("`JdbcShardedWorkRegistry` should")
public class JdbcShardedWorkRegistryTest extends ShardedWorkRegistryTest {

    private static final ShardIndex index = newIndex(1, 15);

    private static final NodeId nodeId = newNode();

    private JdbcShardedWorkRegistry registry;

    @BeforeEach
    void setUp() {
        DataSourceWrapper dataSource = whichIsStoredInMemory("jdbcShardedWorkRegistryTest");
        JdbcStorageFactory storageFactory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(H2_2_1)
                .build();
        registry = new JdbcShardedWorkRegistry(storageFactory);
    }

    @Override
    protected ShardedWorkRegistry registry() {
        return registry;
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .setDefault(NodeId.class, newNode())
                .setDefault(ShardIndex.class, newIndex(4, 5))
                .setDefault(ShardSessionRecord.class, ShardSessionRecord.getDefaultInstance())
                .setDefault(ShardSessionReadRequest.class,
                            new ShardSessionReadRequest(newIndex(6, 10)))
                .testAllPublicInstanceMethods(registry);
    }

    @Test
    @DisplayName("pick up the shard and write a corresponding record to the storage")
    void pickUp() {
        PickUpOutcome outcome = registry.pickUp(index, nodeId);
        assertThat(outcome.hasSession()).isTrue();
        assertThat(outcome.getSession().getIndex()).isEqualTo(index);

        ShardSessionRecord record = readSingleRecord(index);
        assertThat(record.getIndex()).isEqualTo(index);

        WorkerId expectedWorker = registry.currentWorkerFor(nodeId);
        assertThat(record.getWorker()).isEqualTo(expectedWorker);
    }

    @Test
    @DisplayName("not be able to pick up the shard if it's already picked up")
    void cannotPickUpIfTaken() {
        PickUpOutcome outcome = registry.pickUp(index, nodeId);
        assertThat(outcome.hasSession()).isTrue();

        PickUpOutcome sameIdxSameNode = registry.pickUp(index, nodeId);
        assertThat(sameIdxSameNode.hasAlreadyPicked()).isTrue();

        PickUpOutcome sameIdxAnotherNode = registry.pickUp(index, newNode());
        assertThat(sameIdxAnotherNode.hasAlreadyPicked()).isTrue();

        ShardIndex anotherIdx = newIndex(24, 100);
        PickUpOutcome anotherIdxSameNode = registry.pickUp(anotherIdx, nodeId);
        assertThat(anotherIdxSameNode.hasSession()).isTrue();

        PickUpOutcome anotherIdxAnotherNode = registry.pickUp(anotherIdx, newNode());
        assertThat(anotherIdxAnotherNode.hasAlreadyPicked()).isTrue();
    }

    @Test
    @DisplayName("complete the shard session (once picked up) and make it available for picking up")
    void completeSessionAndMakeItAvailable() {
        PickUpOutcome outcome = registry.pickUp(index, nodeId);
        assertThat(outcome.hasSession()).isTrue();

        Timestamp whenPickedFirst = readSingleRecord(index).getWhenLastPicked();

        ShardSessionRecord session = outcome.getSession();
        registry.release(session);

        ShardSessionRecord completedRecord = readSingleRecord(index);
        assertThat(completedRecord.hasWorker()).isFalse();

        // On some platforms (namely some Windows distributions), Java cannot obtain current time
        // with enough precision to compare timestamps in this test. By waiting for 1 second, we
        // ensure that the timestamps will not accidentally be identical.
        sleepUninterruptibly(Duration.ofSeconds(1));

        NodeId anotherNode = newNode();
        PickUpOutcome anotherOutcome = registry.pickUp(index, anotherNode);
        assertThat(anotherOutcome.hasSession()).isTrue();

        ShardSessionRecord secondSessionRecord = readSingleRecord(index);
        WorkerId worker = registry.currentWorkerFor(anotherNode);
        assertThat(secondSessionRecord.getWorker()).isEqualTo(worker);

        Timestamp whenPickedSecond = secondSessionRecord.getWhenLastPicked();
        assertThat(Timestamps.compare(whenPickedFirst, whenPickedSecond) < 0).isTrue();
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")    /* Checked via Truth8 API. */
    private ShardSessionRecord readSingleRecord(ShardIndex index) {
        Optional<ShardSessionRecord> record = registry.find(index);
        assertThat(record).isPresent();

        return record.get();
    }

    private static NodeId newNode() {
        return NodeId.newBuilder()
                     .setValue(Identifier.newUuid())
                     .vBuild();
    }
}
