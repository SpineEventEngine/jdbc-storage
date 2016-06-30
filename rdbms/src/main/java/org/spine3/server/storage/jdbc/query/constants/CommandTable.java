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

package org.spine3.server.storage.jdbc.query.constants;

import com.google.protobuf.Descriptors;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.util.IdColumn;

/**
 * A utility class representing constants which are necessary for working with command table.
 *
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
public class CommandTable {

    /**
     * Commands table name.
     */
    public static final String TABLE_NAME = "commands";

    /**
     * Command ID column name.
     */
    public static final String ID_COL = "id";

    /**
     * Command record column name.
     */
    public static final String COMMAND_COL = "command";

    /**
     * Is command status column name.
     */
    public static final String COMMAND_STATUS_COL = "command_status";

    /**
     * Command error column name.
     */
    public static final String ERROR_COL = "error";

    /**
     * Command failure column name.
     */
    public static final String FAILURE_COL = "failure";

    /**
     * Record descriptor for Command record type.
     */
    public static final Descriptors.Descriptor COMMAND_RECORD_DESCRIPTOR = CommandStorageRecord.getDescriptor();

    /**
     * Record descriptor for Error record type.
     */
    public static final Descriptors.Descriptor ERROR_DESCRIPTOR = Error.getDescriptor();

    /**
     * Record descriptor for Failure record type.
     */
    public static final Descriptors.Descriptor FAILURE_DESCRIPTOR = Failure.getDescriptor();

    public static final IdColumn.StringIdColumn STRING_ID_COLUMN = new IdColumn.StringIdColumn();

    private CommandTable() {
    }
}
