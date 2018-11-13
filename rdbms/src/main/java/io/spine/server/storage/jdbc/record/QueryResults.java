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
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.PairedValue;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageColumn;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ENTITY;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ID;

/**
 * Utility class for parsing the results of a DB query ({@link ResultSet}) into the
 * required in-memory representation.
 *
 * @author Dmytro Dashenkov
 */
final class QueryResults {

    /** Prevents the utility class instantiation. */
    private QueryResults() {
    }

    /**
     * Creates an {@code Iterator} over the results of the SQL query.
     *
     * <p>The results are represented as ID-to-{@code EntityRecord} {@link PairedValue}.
     *
     * @param resultSet
     *         the result of the query
     * @param idType
     *         the type of the queried entity ID
     * @param fieldMask
     *         the {@code FieldMask} to apply to the results
     * @param <I>
     *         the compile-time type of the queried entity ID
     * @return an {@code Iterator} over the query results
     * @see RecordTable
     */
    static <I> Iterator<PairedValue<I, EntityRecord>>
    parse(ResultSet resultSet, Class<I> idType, FieldMask fieldMask) {
        Iterator<PairedValue<I, EntityRecord>> dbIterator = DbIterator.createFor(
                resultSet,
                idColumn(ID.name(), idType),
                messageColumn(ENTITY.name(), EntityRecord.getDescriptor())
        );
        Iterator<PairedValue<I, EntityRecord>> result = stream(dbIterator)
                .map(maskFields(fieldMask))
                .iterator();
        return result;
    }

    private static <I> Function<PairedValue<I, EntityRecord>, PairedValue<I, EntityRecord>>
    maskFields(FieldMask fieldMask) {
        return value -> {
            checkNotNull(value);
            EntityRecord record = value.bValue();
            Any maskedState = maskFieldsOfState(record, fieldMask);
            EntityRecord maskedRecord = record.toBuilder()
                                              .setState(maskedState)
                                              .build();
            return PairedValue.of(value.aValue(), maskedRecord);
        };
    }

    private static Any maskFieldsOfState(EntityRecord record, FieldMask fieldMask) {
        Message message = AnyPacker.unpack(record.getState());
        Message result = FieldMasks.applyMask(fieldMask, message);
        return AnyPacker.pack(result);
    }
}
