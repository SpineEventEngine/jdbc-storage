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

import io.spine.base.Identifier;
import io.spine.server.NodeId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.message.JdbcMessageStorageTest;
import org.junit.jupiter.api.DisplayName;

import static io.spine.base.Time.currentTime;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.delivery.given.TestShardIndex.newIndex;

@DisplayName("JdbcShardedWorkRegistry should")
class JdbcShardedWorkRegistryTest extends JdbcMessageStorageTest<ShardIndex,
                                                                 ShardSessionRecord,
                                                                 ShardSessionReadRequest,
                                                                 JdbcShardedWorkRegistry> {

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
}
