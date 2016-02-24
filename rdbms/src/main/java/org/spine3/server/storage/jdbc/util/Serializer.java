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

package org.spine3.server.storage.jdbc.util;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.spine3.Internal;
import org.spine3.type.TypeName;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.protobuf.Messages.fromAny;
import static org.spine3.protobuf.Messages.toAny;

/**
 * A utility class for serializing/deserializing messages.
 *
 * @author Alexander Litus
 */
@Internal
@SuppressWarnings("UtilityClass")
public class Serializer {

    private Serializer() {}

    /**
     * Deserializes a {@link Message}.
     *
     * @param bytes a serialized message
     * @param messageDescriptor a descriptor of a message
     * @param <M> a type of message expected
     * @return a message instance
     */
    public static <M extends Message> M deserializeMessage(byte[] bytes, Descriptor messageDescriptor) {
        checkNotNull(bytes);
        final Any.Builder builder = Any.newBuilder();
        final String typeUrl = TypeName.of(messageDescriptor).toTypeUrl();
        builder.setTypeUrl(typeUrl);
        final ByteString byteString = ByteString.copyFrom(bytes);
        builder.setValue(byteString);
        final M message = fromAny(builder.build());
        return message;
    }

    /**
     * Serializes a message to an array of bytes.
     *
     * @param message a message to serialize
     * @param <M> a message type
     * @return a byte array
     */
    public static <M extends Message> byte[] serialize(M message) {
        checkNotNull(message);
        final Any any = toAny(message);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }
}
