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

package io.spine.server.storage.jdbc;

import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.type.TypeUrl;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.Serializer.deserialize;

/**
 * An iterator over the message records of a table.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public class MessageDbIterator<M extends Message> extends DbIterator<M> {

    private final TypeUrl recordType;

    public MessageDbIterator(ResultSet resultSet, String columnName, TypeUrl recordType) {
        super(resultSet, columnName);
        this.recordType = recordType;
    }

    @Override
    protected M readResult() throws SQLException {
        final byte[] bytes = getResultSet().getBytes(getColumnName());
        final M result = deserialize(bytes, recordType);
        return result;
    }
}
