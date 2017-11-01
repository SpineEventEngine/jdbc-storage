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

package io.spine.server.storage.jdbc.query;

import com.google.common.base.Function;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.FieldMasks;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.server.storage.jdbc.MessageDbIterator;
import io.spine.type.TypeUrl;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.transform;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.entity;
import static io.spine.type.TypeUrl.from;

/**
 * Utility class for parsing the results of a DB query ({@link ResultSet}) into the
 * required in-memory representation.
 *
 * @author Dmytro Dashenkov
 */
final class QueryResults {

    private static final TypeUrl ENTITY_RECORD_TYPE_URL = from(EntityRecord.getDescriptor());

    private QueryResults() {
        // Prevent utility class instantiation.
    }

    /**
     * Transforms results of SQL query results into ID-to-{@link EntityRecord} {@link Map}.
     *
     * @param resultSet Results of the query
     * @param fieldMask {@code FieldMask} to apply to the results
     * @return ID-to-{@link EntityRecord} {@link Map} representing the query results
     * @see RecordTable
     */
    static Iterator<EntityRecord> parse(ResultSet resultSet, FieldMask fieldMask) {
        final Iterator<EntityRecord> recordIterator =
                new MessageDbIterator<>(resultSet, entity.name(), ENTITY_RECORD_TYPE_URL);
        final Iterator<EntityRecord> result = transform(recordIterator, maskFields(fieldMask));
        return result;
    }

    private static Any maskFieldsOfState(EntityRecord record, FieldMask fieldMask) {
        final Message message = AnyPacker.unpack(record.getState());
        final TypeUrl typeUrl = from(message.getDescriptorForType());
        final Message result = FieldMasks.applyMask(fieldMask, message, typeUrl);
        return AnyPacker.pack(result);
    }

    private static Function<EntityRecord, EntityRecord> maskFields(final FieldMask fieldMask) {
        return new Function<EntityRecord, EntityRecord>() {
            @Override
            public EntityRecord apply(@Nullable EntityRecord entityRecord) {
                checkNotNull(entityRecord);
                final Any maskedState = maskFieldsOfState(entityRecord, fieldMask);
                final EntityRecord result = entityRecord.toBuilder()
                                                        .setState(maskedState)
                                                        .build();
                return result;
            }
        };
    }
}
