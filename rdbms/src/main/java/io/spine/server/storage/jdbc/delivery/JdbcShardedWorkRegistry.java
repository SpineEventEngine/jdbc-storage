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

import io.spine.server.NodeId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.message.JdbcMessageStorage;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.base.Time.currentTime;
import static io.spine.util.Exceptions.unsupported;

public class JdbcShardedWorkRegistry
        extends JdbcMessageStorage<ShardIndex,
                                   ShardSessionRecord,
                                   ShardSessionReadRequest,
                                   ShardedWorkRegistryTable>
        implements ShardedWorkRegistry {

    private JdbcShardedWorkRegistry(Builder builder) {
        super(false,
              new ShardedWorkRegistryTable(builder.getDataSource(), builder.getTypeMapping()));
    }

    @Override
    public Optional<ShardProcessingSession> pickUp(ShardIndex index, NodeId nodeId) {
        checkNotNull(index);
        checkNotNull(nodeId);
        checkNotClosed();

        boolean pickedAlready = isPickedAlready(index);
        if (pickedAlready) {
            return Optional.empty();
        }
        ShardSessionRecord ssr = newRecord(index, nodeId);
        write(ssr);

        JdbcShardProcessingSession result =
                new JdbcShardProcessingSession(ssr, () -> clearNode(ssr));
        return Optional.of(result);
    }

    private boolean isPickedAlready(ShardIndex index) {
        ShardSessionRecord record = table().read(index);
        boolean alreadyPicked = record != null && record.hasPickedBy();
        return alreadyPicked;
    }

    private static ShardSessionRecord newRecord(ShardIndex index, NodeId nodeId) {
        return ShardSessionRecord.newBuilder()
                                 .setIndex(index)
                                 .setPickedBy(nodeId)
                                 .setWhenLastPicked(currentTime())
                                 .vBuild();
    }

    private void clearNode(ShardSessionRecord record) {
        ShardSessionRecord updated = record.toBuilder()
                                           .clearPickedBy()
                                           .vBuild();
        write(updated);
    }

    public static class Builder extends StorageBuilder<Builder, JdbcShardedWorkRegistry> {

        @Override
        public Builder setMultitenant(boolean multitenant) {
            throw unsupported("`JdbcShardedWorkRegistry` is an application-wide instance " +
                              "and therefore always single-tenant");
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected JdbcShardedWorkRegistry doBuild() {
            return new JdbcShardedWorkRegistry(this);
        }
    }
}
