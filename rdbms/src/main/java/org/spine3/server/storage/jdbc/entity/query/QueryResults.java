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

package org.spine3.server.storage.jdbc.entity.query;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.storage.jdbc.table.entity.RecordTable.Column;
import org.spine3.server.storage.jdbc.util.Serializer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.spine3.server.storage.jdbc.table.entity.RecordTable.Column.*;

/**
 * Utility class for parsing the results of a DB query ({@link ResultSet}) into the required in-memory representation.
 *
 * @author Dmytro Dashenkov
 */
@SuppressWarnings("UtilityClass")
class QueryResults {

    private QueryResults() {
    }

    /**
     * Transforms results of SQL query into ID-to-{@link EntityRecord} {@link Map}.
     *
     * @param resultSet Results of the query.
     * @param fieldMask {@code FieldMask} to apply to the results.
     * @param <I>      ID type of the {@link org.spine3.server.entity.Entity}.
     * @param <S>   S type of the {@link org.spine3.server.entity.Entity}.
     * @return ID-to-{@link EntityRecord} {@link Map} representing the query results.
     * @throws SQLException if read results contain no ID column or entity column.
     * @see org.spine3.server.storage.jdbc.table.entity.RecordTable
     */
    static <I, S extends Message> Map<I, EntityRecord> parse(
            ResultSet resultSet,
            FieldMask fieldMask)
            throws SQLException {
        final ImmutableMap.Builder<I, EntityRecord> resultBuilder = new ImmutableMap.Builder<>();

        while (resultSet.next()) {
            final EntityRecord record = readSingleMessage(resultSet);
            final S maskedMessage = maskFields(record, fieldMask);
            @SuppressWarnings("unchecked")
            final I id = (I) resultSet.getObject(Column.id.name());
            resultBuilder.put(id, EntityRecord.newBuilder(record)
                                              .setState(AnyPacker.pack(maskedMessage))
                                              .build());
        }

        resultSet.close();

        return resultBuilder.build();
    }

    private static EntityRecord readSingleMessage(ResultSet resultSet) throws SQLException {
        return Serializer.deserialize(resultSet.getBytes(entity.name()),
                                      EntityRecord.getDescriptor());
    }

    private static <M extends Message> M maskFields(EntityRecord record, FieldMask fieldMask) {
        final M message = AnyPacker.unpack(record.getState());
        final TypeUrl typeUrl = TypeUrl.from(message.getDescriptorForType());
        return FieldMasks.applyMask(fieldMask, message, typeUrl);
    }
}
