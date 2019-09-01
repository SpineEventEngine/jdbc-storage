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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import io.spine.logging.Logging;
import io.spine.server.NodeId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.message.JdbcMessageStorage;
import io.spine.string.Stringifiers;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;
import static com.google.protobuf.util.Timestamps.between;
import static io.spine.base.Time.currentTime;
import static io.spine.util.Exceptions.unsupported;

/**
 * A JDBC-based implementation of the {@link ShardedWorkRegistry}.
 *
 * <p>Represents an SQL table of {@linkplain ShardSessionRecord session records} with the
 * appropriate accessor methods.
 */
public class JdbcShardedWorkRegistry
        extends JdbcMessageStorage<ShardIndex,
                                   ShardSessionRecord,
                                   ShardSessionReadRequest,
                                   ShardedWorkRegistryTable>
        implements ShardedWorkRegistry, Logging {

    private JdbcShardedWorkRegistry(Builder builder) {
        super(false,
              new ShardedWorkRegistryTable(builder.getDataSource(), builder.getTypeMapping()));
    }

    @Override
    public synchronized Optional<ShardProcessingSession> pickUp(ShardIndex index, NodeId nodeId) {
        checkNotNull(index);
        checkNotNull(nodeId);
        checkNotClosed();

        boolean pickedAlready = isPickedAlready(index);
        if (pickedAlready) {
            return Optional.empty();
        }
        ShardSessionRecord ssr = newRecord(index, nodeId);
        write(ssr);

        boolean writeConsistent = checkConsistency(index, ssr);
        if (writeConsistent) {
            ShardProcessingSession result = new JdbcShardProcessingSession(ssr, () -> clearNode(ssr));
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Iterable<ShardIndex> releaseExpiredSessions(Duration inactivityPeriod) {
        checkNotNull(inactivityPeriod);

        ImmutableList<ShardSessionRecord> allRecords =  ImmutableList.copyOf(readAll());
        ImmutableList.Builder<ShardIndex> resultBuilder = ImmutableList.builder();
        for (ShardSessionRecord record : allRecords) {
            Timestamp whenPicked = record.getWhenLastPicked();
            Duration elapsed = between(whenPicked, currentTime());

            int comparison = Durations.compare(elapsed, inactivityPeriod);
            if (comparison >= 0) {
                clearNode(record);
                resultBuilder.add(record.getIndex());
            }
        }
        return resultBuilder.build();
    }

    private boolean checkConsistency(ShardIndex index, ShardSessionRecord ssr) {
        List<ShardSessionRecord> records = ImmutableList.copyOf(readByIndex(index));
        if (records.size() > 1) {
            _warn().log("Several `ShardSessionRecord`s found for index %s.",
                        Stringifiers.toString(index));
        }
        ShardSessionRecord fromStorage = records.iterator()
                                                .next();
        return fromStorage.equals(ssr);
    }

    /**
     * Tells if the session with the passed index is currently picked by any node.
     */
    private boolean isPickedAlready(ShardIndex index) {
        Iterator<ShardSessionRecord> records = readByIndex(index);
        long nodesWithShard = stream(records)
                .filter(ShardSessionRecord::hasPickedBy)
                .count();
        return nodesWithShard > 0;
    }

    /**
     * Reads all messages belonging to a {@linkplain ShardIndex#getIndex() shard index}.
     */
    @VisibleForTesting
    Iterator<ShardSessionRecord> readByIndex(ShardIndex index) {
        return table().readByIndex(index);
    }

    /**
     * Reads all messages from the storage.
     */
    private Iterator<ShardSessionRecord> readAll() {
        return table().readAll();
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

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends StorageBuilder<Builder, JdbcShardedWorkRegistry> {

        @Override
        public Builder setMultitenant(boolean multitenant) {
            throw unsupported("`JdbcShardedWorkRegistry` is an application-wide instance " +
                              "and therefore is always single-tenant");
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
