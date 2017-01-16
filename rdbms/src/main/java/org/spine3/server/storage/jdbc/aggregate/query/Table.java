/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

/**
 * A utility class representing constants which are necessary for working with aggregate tables.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
final class Table {

    private static final String ID_COL = "id";

    /** Table  that contains aggregate records. */
    static class AggregateRecord {

        /** ID column name. */
        static final String ID_COL = Table.ID_COL;

        /** Aggregate record column name. */
        static final String AGGREGATE_COL = "aggregate";

        /** Aggregate event seconds column name. */
        @SuppressWarnings("DuplicateStringLiteralInspection")
        static final String SECONDS_COL = "seconds";

        /** Aggregate event nanoseconds column name. */
        @SuppressWarnings("DuplicateStringLiteralInspection")
        static final String NANOS_COL = "nanoseconds";

        private AggregateRecord() {
        }
    }

    /**
     * Table that contains counts of events, which were saved to the storage
     * after the last snapshot of the corresponding aggregate was created,
     * or a count of all events if there were no snapshots yet.
     */
    static class EventCount {

        /** Aggregate ID column name. */
        static final String ID_COL = Table.ID_COL;

        /** A count of events after the last snapshot column name. */
        static final String EVENT_COUNT_COL = "event_count";

        /** A suffix for this table name. */
        static final String EVENT_COUNT_TABLE_NAME_SUFFIX = "_event_count";

        private EventCount() {
        }
    }

    private Table() {}
}
