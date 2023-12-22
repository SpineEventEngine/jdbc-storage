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

import com.google.protobuf.Duration;
import io.spine.logging.WithLogging;
import io.spine.server.ContextSpec;
import io.spine.server.NodeId;
import io.spine.server.delivery.AbstractWorkRegistry;
import io.spine.server.delivery.PickUpOutcome;
import io.spine.server.delivery.ShardAlreadyPickedUp;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.delivery.WorkerId;
import io.spine.server.storage.jdbc.JdbcStorageFactory;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.type.Json.toCompactJson;
import static io.spine.util.Exceptions.newIllegalStateException;

/**
 * A JDBC-based implementation of the {@link ShardedWorkRegistry}.
 *
 * <p>Represents an SQL table of {@linkplain ShardSessionRecord session records} with the
 * appropriate accessor methods.
 */
public class JdbcShardedWorkRegistry extends AbstractWorkRegistry implements WithLogging {

    private final JdbcSessionStorage storage;

    /**
     * Creates a new registry.
     *
     * @param storageFactory
     *         the storage factory for creating a storage for this registry
     * @param context
     *         specification of the Bounded Context in which the created storage will reside
     * @see io.spine.server.delivery.Delivery#contextSpec(boolean)
     */
    public JdbcShardedWorkRegistry(JdbcStorageFactory storageFactory, ContextSpec context) {
        super();
        checkNotNull(storageFactory);
        checkNotNull(context);
        this.storage = storageFactory.createSessionStorage(context);
    }

    @Override
    public synchronized PickUpOutcome pickUp(ShardIndex index, NodeId nodeId) {
        var outcome = super.pickUp(index, nodeId);
        if(outcome.hasAlreadyPicked()) {
            return outcome;
        }
        var result = doubleCheckPickUp(index, nodeId, outcome);
        return result;
    }

    private PickUpOutcome doubleCheckPickUp(ShardIndex index, NodeId nodeId, PickUpOutcome outcome) {
        var worker = currentWorkerFor(nodeId);
        var record = find(index);

        var actualRecord = ensureRecordPresent(record, index, worker);
        var actualWorker = actualRecord.getWorker();
        var pickUpSuccessful = actualWorker.equals(worker);
        if (pickUpSuccessful) {
            return outcome;
        }
        var negativeOutcome = pickedUpBySomeoneElse(outcome, actualRecord);
        return negativeOutcome;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") /* Checking the `Optional`. */
    private static ShardSessionRecord
    ensureRecordPresent(Optional<ShardSessionRecord> record, ShardIndex index, WorkerId worker) {
        if(record.isEmpty()) {
            throw newIllegalStateException(
                    "Storage did not store the last shard pick-up attempt " +
                            "for worker `%s` and shard index `%s`.",
                    toCompactJson(worker), toCompactJson(index));
        }
        return record.get();
    }

    private static PickUpOutcome
    pickedUpBySomeoneElse(PickUpOutcome outcome, ShardSessionRecord actualRecord) {
        var alreadyPickedUp =
                ShardAlreadyPickedUp.newBuilder()
                        .setWhenPicked(actualRecord.getWhenLastPicked())
                        .setWorker(actualRecord.getWorker())
                        .build();
        var result = outcome.toBuilder()
                .setAlreadyPicked(alreadyPickedUp)
                .build();
        return result;
    }

    @Override
    public void release(ShardSessionRecord record) {
        clearNode(record);
    }

    /**
     * Creates a worker ID by combining the given node ID with the ID of the current Java thread,
     * in which the execution in performed.
     */
    @Override
    protected WorkerId currentWorkerFor(NodeId node) {
        var threadId = Thread.currentThread().getId();
        return WorkerId
                .newBuilder()
                .setNodeId(node)
                .setValue(Long.toString(threadId))
                .build();
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
        return storage.read(index);
    }
}
