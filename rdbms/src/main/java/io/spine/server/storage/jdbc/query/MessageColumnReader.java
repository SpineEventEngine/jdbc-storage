/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Message;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.json.Json.fromJson;

/**
 * The reader for the columns which store {@link Message} values in JSON format.
 *
 * <p>The result of the read operation is always a Protobuf {@link Message}.
 *
 * @param <M>
 *         the type of the column
 * @see io.spine.json.Json Json
 */
final class MessageColumnReader<M extends Message> extends ColumnReader<M> {

    private final Class<M> messageClass;

    /**
     * Creates a new {@code MessageColumnReader} instance.
     *
     * @param columnName
     *         the name of the column to read
     * @param messageClass
     *         the type of messages stored in the column
     */
    MessageColumnReader(String columnName, Class<M> messageClass) {
        super(columnName);
        this.messageClass = checkNotNull(messageClass);
    }

    @Override
    public M readValue(ResultSet resultSet) throws SQLException {
        checkNotNull(resultSet);
        String messageJson = resultSet.getString(columnName());
        M result = fromJson(messageJson, messageClass);
        return result;
    }
}
