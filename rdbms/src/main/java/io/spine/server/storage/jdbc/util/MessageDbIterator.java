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

package io.spine.server.storage.jdbc.util;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.type.TypeUrl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.util.Serializer.deserialize;

/**
 * @author Dmytro Dashenkov
 */
public class MessageDbIterator<M extends Message> extends DbIterator<M> {

    private final TypeUrl recordType;

    /**
     * Creates a new iterator instance.
     *
     * @param statement  a statement used to retrieve a result set
     *                   (both statement and result set are closed in {@link #close()}).
     * @param columnName the name of a serialized storage record column
     * @param recordType the type of the storage record
     * @throws DatabaseException if an error occurs during interaction with the DB
     */
    public MessageDbIterator(PreparedStatement statement, String columnName, TypeUrl recordType)
            throws DatabaseException {
        super(statement, columnName);
        this.recordType = recordType;
    }

    @Override
    protected M readResult() throws SQLException {
        final byte[] bytes = getResultSet().getBytes(getColumnName());
        final M result = deserialize(bytes, recordType);
        return result;
    }
}
