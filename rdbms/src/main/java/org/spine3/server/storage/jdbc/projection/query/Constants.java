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

package org.spine3.server.storage.jdbc.projection.query;

/**
 * A utility class representing constants which are necessary for working with projection table.
 *
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
/* package */ class Constants {

    /**
     * Last event time seconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String SECONDS_COL = "seconds";

    /**
     * Last event time nanoseconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String NANOS_COL = "nanoseconds";

    /**
     * A suffix of a table name where the last event time is stored.
     */
    public static final String LAST_EVENT_TIME_TABLE_NAME_SUFFIX = "_last_event_time";

    private Constants() {
    }
}
