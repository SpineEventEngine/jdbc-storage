package org.spine3.server.storage.jdbc.query.constants;

import com.google.protobuf.Descriptors;
import org.spine3.server.storage.EventStorageRecord;

@SuppressWarnings("UtilityClass")
public class EventTable {
    public static final String TABLE_NAME = "events";

    /**
     * Event ID column name.
     */
    public static final String EVENT_ID_COL = "event_id";

    /**
     * Event record column name.
     */
    public static final String EVENT_COL = "event";

    /**
     * Protobuf type name of the event column name.
     */
    public static final String EVENT_TYPE_COL = "event_type";

    /**
     * Producer ID column name.
     */
    public static final String PRODUCER_ID_COL = "producer_id";

    /**
     * Event seconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String SECONDS_COL = "seconds";

    /**
     * Event nanoseconds column name.
     */
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String NANOSECONDS_COL = "nanoseconds";

    @SuppressWarnings("DuplicateStringLiteralInspection")
    public static final String SELECT_EVENT_FROM_TABLE = "SELECT " + EVENT_COL + " FROM " + TABLE_NAME + ' ';

    public static final Descriptors.Descriptor RECORD_DESCRIPTOR = EventStorageRecord.getDescriptor();

    private EventTable() {
    }
}
