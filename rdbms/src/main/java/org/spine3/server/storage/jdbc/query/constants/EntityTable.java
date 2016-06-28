package org.spine3.server.storage.jdbc.query.constants;

import com.google.protobuf.Descriptors;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.util.IdColumn;

public class EntityTable {

    /**
     * Entity record column name.
     */
    public static final String ENTITY_COL = "entity";

    /**
     * Entity ID column name.
     */
    public static final String ID_COL = "id";

    public static final Descriptors.Descriptor RECORD_DESCRIPTOR = EntityStorageRecord.getDescriptor();

}
