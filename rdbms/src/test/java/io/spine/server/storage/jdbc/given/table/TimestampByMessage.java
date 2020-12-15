/*
 * Copyright 2020, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc.given.table;

import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Holds {@link Timestamp} records by {@code StringValue} IDs.
 */
public final class TimestampByMessage extends TimestampTable<StringValue> {

    private static final String NAME = "timestamp_by_message";

    public TimestampByMessage(DataSourceWrapper dataSource, TypeMapping typeMapping) {
        super(NAME, IdColumn.of(TheIdColumn.INSTANCE, StringValue.class), dataSource, typeMapping);
    }

    public enum TheIdColumn implements MessageTable.Column<Timestamp> {
        INSTANCE;

        @Override
        public Getter<Timestamp> getter() {
            return TheIdColumn::idOf;
        }

        @Override
        public @Nullable Type type() {
            return null;
        }

        @Override
        public boolean isPrimaryKey() {
            return true;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        private static StringValue idOf(Timestamp timestamp) {
            StringValue result = StringValue
                    .newBuilder()
                    .setValue(Timestamps.toString(timestamp))
                    .build();
            return result;
        }
    }
}
