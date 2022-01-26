/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.util.Timestamps;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateEventRecordColumn;
import io.spine.server.storage.jdbc.record.column.ColumnSpec;
import io.spine.server.storage.jdbc.record.column.CustomColumns;

import static io.spine.server.storage.jdbc.record.column.ColumnSpec.columnSpec;

/**
 * The columns of {@link AggregateEventRecord} customized for storing in RDBMS.
 */
public final class AggregateEventRecordColumns extends CustomColumns<AggregateEventRecord> {

    private static final ImmutableList<ColumnSpec<AggregateEventRecord, ?>> columns =
            ImmutableList.of(
                    // This is because `Timestamps.MIN_VALUE` and `Timestamps.MAX_VALUE`
                    // are used in Aggregate tests, and they are way too large
                    // when converted to nanoseconds.
                    columnSpec(AggregateEventRecordColumn.created, Timestamps::toMicros)
            );

    private static final AggregateEventRecordColumns instance =
            new AggregateEventRecordColumns(columns);

    private AggregateEventRecordColumns(ImmutableList<ColumnSpec<AggregateEventRecord, ?>> cols) {
        super(cols);
    }

    public static AggregateEventRecordColumns instance() {
        return instance;
    }
}
