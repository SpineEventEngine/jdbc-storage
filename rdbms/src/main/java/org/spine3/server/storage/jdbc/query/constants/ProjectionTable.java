package org.spine3.server.storage.jdbc.query.constants;

public class ProjectionTable {

    /**
     * A suffix of a table name where the last event time is stored.
     */
    public static final String LAST_EVENT_TIME_TABLE_NAME_SUFFIX = "_last_event_time";

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
}
