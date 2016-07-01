/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.aggregate.query;

import com.google.protobuf.Descriptors;
import org.spine3.server.storage.AggregateStorageRecord;

/**
 * A utility class representing constants which are necessary for working with aggregate tables.
 *
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
/* package */ final class Constants {

    /**
     * Aggregate ID column name (contains in `main` and `event_count` tables).
     */
    /* package */ static final String ID_COL = "id";

    /**
     * Aggregate record column name.
     */
    /* package */ static final String AGGREGATE_COL = "aggregate";

    /**
     * Aggregate event seconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    /* package */ static final String SECONDS_COL = "seconds";

    /**
     * Aggregate event nanoseconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    /* package */ static final String NANOS_COL = "nanoseconds";

    /**
     * A count of events after the last snapshot column name.
     */
    /* package */ static final String EVENT_COUNT_COL = "event_count";

    /**
     * A suffix of a table name where the last event time is stored.
     */
    /* package */ static final String EVENT_COUNT_TABLE_NAME_SUFFIX = "_event_count";

    /**
     * Record descriptor for specified record type.
     */
    /* package */ static final Descriptors.Descriptor RECORD_DESCRIPTOR = AggregateStorageRecord.getDescriptor();

    private Constants() {
    }
}
