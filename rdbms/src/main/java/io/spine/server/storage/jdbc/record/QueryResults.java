/*
 * Copyright 2018, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record;

import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.FieldMasks;
import io.spine.server.storage.jdbc.query.IdWithMessage;
import io.spine.server.storage.jdbc.query.IndexWithMessageIterator;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ENTITY;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ID;

/**
 * Utility class for parsing the results of a DB query ({@link ResultSet}) into the
 * required in-memory representation.
 *
 * @author Dmytro Dashenkov
 */
final class QueryResults {

    private QueryResults() {
        // Prevent utility class instantiation.
    }

    /**
     * Transforms results of SQL query results into ID-to-{@link EntityRecord} {@link Map}.
     *
     * @param resultSet Results of the query
     * @param idType
     * @param fieldMask {@code FieldMask} to apply to the results
     * @return ID-to-{@link EntityRecord} {@link Map} representing the query results
     * @see RecordTable
     */
    @SuppressWarnings("unchecked")
    static <I> Iterator<IdWithMessage<I, EntityRecord>>
    parse(ResultSet resultSet, Class<I> idType, FieldMask fieldMask) {
        IndexWithMessageIterator<I, EntityRecord> iterator = IndexWithMessageIterator
                .newBuilder()
                .setResultSet(resultSet)
                .setColumnName(ENTITY.name())
                .setIdColumnName(ID.name())
                .setIdType(idType)
                .setMessageDescriptor(EntityRecord.getDescriptor())
                .build();
        Iterator<IdWithMessage<I, EntityRecord>> result = stream(iterator)
                .map(maskFields(fieldMask))
                .iterator();
        return result;
    }

    private static Any maskFieldsOfState(EntityRecord record, FieldMask fieldMask) {
        Message message = AnyPacker.unpack(record.getState());
        Message result = FieldMasks.applyMask(fieldMask, message);
        return AnyPacker.pack(result);
    }

    private static <I> Function<IdWithMessage<I, EntityRecord>, IdWithMessage<I, EntityRecord>>
    maskFields(FieldMask fieldMask) {
        return idWithMessage -> {
            checkNotNull(idWithMessage);
            EntityRecord message = idWithMessage.message();
            Any maskedState = maskFieldsOfState(message, fieldMask);
            EntityRecord newRecord = message.toBuilder()
                                         .setState(maskedState)
                                         .build();
            IdWithMessage<I, EntityRecord> result =
                    new IdWithMessage<>(idWithMessage.id(), newRecord);
            return result;
        };
    }
}
