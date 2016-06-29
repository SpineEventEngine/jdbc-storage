package org.spine3.server.storage.jdbc.query.constants;


import com.google.protobuf.Descriptors;
import org.spine3.server.storage.AggregateStorageRecord;

public class AggregateTable {
    /**
     * Aggregate ID column name (contains in `main` and `event_count` tables).
     */
    public static final String ID_COL = "id";

    /**
     * Aggregate record column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String AGGREGATE_COL = "aggregate";

    /**
     * Aggregate event seconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String SECONDS_COL = "seconds";

    /**
     * Aggregate event nanoseconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String NANOS_COL = "nanoseconds";

    /**
     * A count of events after the last snapshot column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String EVENT_COUNT_COL = "event_count";

    /**
     * A suffix of a table name where the last event time is stored.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String EVENT_COUNT_TABLE_NAME_SUFFIX = "_event_count";

    public static final Descriptors.Descriptor RECORD_DESCRIPTOR = AggregateStorageRecord.getDescriptor();


    private AggregateTable() {
    }
}
