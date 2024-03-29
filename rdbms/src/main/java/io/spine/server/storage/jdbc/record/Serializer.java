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

package io.spine.server.storage.jdbc.record;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.protobuf.AnyPacker;
import io.spine.type.TypeUrl;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility class for serializing/deserializing Protobuf records.
 */
@Internal
public final class Serializer {

    private Serializer() {
        // Prevent utility class instantiation.
    }

    /**
     * Serializes a message to an array of bytes.
     *
     * @param message
     *         the message to serialize
     * @return a byte array
     */
    public static byte[] serialize(Message message) {
        checkNotNull(message);
        var any = AnyPacker.pack(message);
        var bytes = any.getValue()
                       .toByteArray();
        return bytes;
    }

    /**
     * Deserializes a {@link Message}.
     *
     * @param bytes
     *         the serialized message
     * @param messageDescriptor
     *         the descriptor of a message
     * @return a message instance
     */
    public static Message deserialize(byte[] bytes, Descriptor messageDescriptor) {
        checkNotNull(bytes);
        checkNotNull(messageDescriptor);
        var builder = Any.newBuilder();
        var typeUrlValue = TypeUrl.from(messageDescriptor)
                                  .value();
        builder.setTypeUrl(typeUrlValue);
        var byteString = ByteString.copyFrom(bytes);
        builder.setValue(byteString);
        var result = AnyPacker.unpack(builder.build());
        return result;
    }
}
