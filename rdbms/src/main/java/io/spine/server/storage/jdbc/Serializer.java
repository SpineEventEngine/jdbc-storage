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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.protobuf.AnyPacker;
import io.spine.type.TypeUrl;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A utility class for serializing/deserializing messages.
 *
 * @author Alexander Litus
 */
@Internal
public final class Serializer {

    private Serializer() {
        // Prevent utility class instantiation.
    }

    /**
     * Serializes a message to an array of bytes.
     *
     * @param message the message to serialize
     * @return a byte array
     */
    public static byte[] serialize(Message message) {
        checkNotNull(message);
        final Any any = AnyPacker.pack(message);
        final byte[] bytes = any.getValue()
                                .toByteArray();
        return bytes;
    }

    /**
     * Deserializes a {@link Message}.
     *
     * @param bytes             the serialized message
     * @param messageDescriptor the descriptor of a message
     * @param <M>               the type of message expected
     * @return a message instance
     */
    public static <M extends Message> M deserialize(byte[] bytes, Descriptor messageDescriptor) {
        checkNotNull(bytes);
        final Any.Builder builder = Any.newBuilder();
        final String typeUrlValue = TypeUrl.from(messageDescriptor)
                                           .value();
        builder.setTypeUrl(typeUrlValue);
        final ByteString byteString = ByteString.copyFrom(bytes);
        builder.setValue(byteString);
        final M message = AnyPacker.unpack(builder.build());
        return message;
    }
}
