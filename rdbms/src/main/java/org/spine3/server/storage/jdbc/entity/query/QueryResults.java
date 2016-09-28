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

package org.spine3.server.storage.jdbc.entity.query;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.storage.jdbc.util.Serializer;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Dmytro Dashenkov
 */
public class QueryResults {

    public static <K, V extends Message> Map<K, V> parse(ResultSet resultSet, Descriptors.Descriptor descriptor, FieldMask fieldMask, TypeUrl typeUrl) throws SQLException {
        final ImmutableMap.Builder<K, V> resultBuilder = new ImmutableMap.Builder<>();

        while (resultSet.next()) {
            final V message = readSingleMessage(resultSet, descriptor);
            final V maskedMessage = maskFields(message, fieldMask, typeUrl);

            @SuppressWarnings("unchecked")
            final K id = (K) resultSet.getObject(EntityTable.ID_COL);

            resultBuilder.put(id, maskedMessage);
        }

        resultSet.close();

        return resultBuilder.build();
    }

    private static <M extends Message> M readSingleMessage(ResultSet resultSet, Descriptors.Descriptor messageDescriptor) throws SQLException {
        return Serializer.deserialize(resultSet.getBytes(EntityTable.ENTITY_COL), messageDescriptor);
    }

    private static  <M extends Message> M maskFields(M message, @Nullable FieldMask fieldMask, TypeUrl typeUrl) {
        if (fieldMask != null) {
            return FieldMasks.applyMask(fieldMask, message, typeUrl);
        }

        return message;
    }
}
