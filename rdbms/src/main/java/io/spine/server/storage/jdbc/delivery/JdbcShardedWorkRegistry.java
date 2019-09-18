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

import com.google.protobuf.Duration;
import io.spine.logging.Logging;
import io.spine.server.NodeId;
import io.spine.server.delivery.AbstractWorkRegistry;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardProcessingSession;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.storage.jdbc.JdbcStorageFactory;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A JDBC-based implementation of the {@link ShardedWorkRegistry}.
 *
 * <p>Represents an SQL table of {@linkplain ShardSessionRecord session records} with the
 * appropriate accessor methods.
 */
public class JdbcShardedWorkRegistry
        extends AbstractWorkRegistry
        implements Logging {

    private final JdbcSessionStorage storage;

    /**
     * Creates a new registry.
     *
     * @param storageFactory
     *         the storage factory for creating a storage for this registry
     */
    public JdbcShardedWorkRegistry(JdbcStorageFactory storageFactory) {
        super();
        checkNotNull(storageFactory);
        this.storage = storageFactory.createSessionStorage();
    }

    @Override
    public synchronized Optional<ShardProcessingSession> pickUp(ShardIndex index, NodeId nodeId) {
        Optional<ShardProcessingSession> picked = super.pickUp(index, nodeId);
        return picked.filter(session -> pickedBy(index, nodeId));
    }

    private boolean pickedBy(ShardIndex index, NodeId nodeId) {
        Optional<ShardSessionRecord> stored = find(index);
        return stored.map(record -> record.getPickedBy().equals(nodeId))
                     .orElse(false);
    }

    @Override
    public synchronized Iterable<ShardIndex> releaseExpiredSessions(Duration inactivityPeriod) {
        return super.releaseExpiredSessions(inactivityPeriod);
    }

    @Override
    protected Iterator<ShardSessionRecord> allRecords() {
        return storage.readAll();
    }

    @Override
    protected void write(ShardSessionRecord session) {
        storage.write(session.getIndex(), session);
    }

    @Override
    protected Optional<ShardSessionRecord> find(ShardIndex index) {
        ShardSessionReadRequest request = new ShardSessionReadRequest(index);
        return storage.read(request);
    }

    @Override
    protected ShardProcessingSession asSession(ShardSessionRecord record) {
        return new JdbcShardProcessingSession(record, () -> clearNode(record));
    }
}
