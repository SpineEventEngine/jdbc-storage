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

package org.spine3.server.storage.jdbc.event.query;

import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;

/**
 * A utility class representing constants which are necessary for working with event table.
 *
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
final class EventTable {

    /** Event table name. */
    static final String TABLE_NAME = "events";

    /** Event ID column name. */
    static final String EVENT_ID_COL = "event_id";

    /** Event record column name. */
    static final String EVENT_COL = "event";

    /** Protobuf type name of the event column name. */
    static final String EVENT_TYPE_COL = "event_type";

    /** Producer ID column name. */
    static final String PRODUCER_ID_COL = "producer_id";

    /** Event seconds column name. */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    static final String SECONDS_COL = "seconds";

    /** Event nanoseconds column name. */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    static final String NANOSECONDS_COL = "nanoseconds";

    @SuppressWarnings("DuplicateStringLiteralInspection")
    static final String SELECT_EVENT_FROM_TABLE = SELECT + EVENT_COL + FROM + TABLE_NAME + ' ';

    private EventTable() {
    }
}
