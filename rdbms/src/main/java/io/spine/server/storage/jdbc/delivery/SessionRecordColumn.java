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

import com.google.common.collect.ImmutableList;
import io.spine.query.RecordColumn;
import io.spine.query.RecordColumns;
import io.spine.server.delivery.ShardSessionRecord;

import static io.spine.query.RecordColumn.create;

/**
 * The definitions of record columns to store along with the {@link ShardSessionRecord}.
 *
 * @apiNote This type is made {@code public} to allow library users query the stored
 *         {@code ShardSessionRecord}s via storage API, in case they need to read
 *         the storage contents manually.
 */
@RecordColumns(ofType = ShardSessionRecord.class)
@SuppressWarnings({"BadImport" /* Using `create` API for columns for brevity.  */,
        "WeakerAccess" /* See API note. */})
public final class SessionRecordColumn {

    public static final RecordColumn<ShardSessionRecord, Long>
            shard = create("SHARD_INDEX", Long.class,
                           (r) -> (long) r.getIndex()
                                          .getIndex());

    public static final RecordColumn<ShardSessionRecord, Long>
            total_shards = create("OF_TOTAL_SHARDS", Long.class,
                                  (r) -> (long) r.getIndex()
                                                 .getOfTotal());

    public static final RecordColumn<ShardSessionRecord, String>
            worker = create("WORKER_ID", String.class,
                            (r) -> {
                                var worker = r.getWorker();
                                var node = worker.getNodeId().getValue();
                                var ownValue = worker.getValue();
                                return node + '-' + ownValue;
                            });

    public static final RecordColumn<ShardSessionRecord, Long>
            when_last_picked = create("WHEN_LAST_PICKED", Long.class,
                                      (r) -> r.getWhenLastPicked()
                                              .getSeconds());

    @SuppressWarnings("ProtoTimestampGetSecondsGetNano")
    public static final RecordColumn<ShardSessionRecord, Integer>
            when_last_picked_nanos = create("WHEN_LAST_PICKED_NANOS", Integer.class,
                                            (r) -> r.getWhenLastPicked()
                                                    .getNanos());

    /**
     * Prevents this type from instantiation.
     *
     * <p>This class exists exclusively as a container of the column definitions. Thus, it isn't
     * expected to be instantiated. See the {@link RecordColumns} docs for more details on
     * this approach.
     */
    private SessionRecordColumn() {
    }

    /**
     * Returns the definitions of all columns.
     */
    public static ImmutableList<RecordColumn<ShardSessionRecord, ?>> definitions() {
        return ImmutableList.of(shard, total_shards, worker,
                                when_last_picked, when_last_picked_nanos);
    }

}
