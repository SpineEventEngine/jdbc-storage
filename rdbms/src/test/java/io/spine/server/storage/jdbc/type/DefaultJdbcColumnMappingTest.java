/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.type;

import com.google.common.testing.NullPointerTester;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.core.Versions;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProject.Status;
import io.spine.test.storage.StgProjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.google.protobuf.util.Timestamps.toNanos;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.type.Json.toCompactJson;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`DefaultJdbcColumnMapping` should")
class DefaultJdbcColumnMappingTest {

    private final DefaultJdbcColumnMapping mapping = new DefaultJdbcColumnMapping();

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .testAllPublicInstanceMethods(mapping);
    }

    @Test
    @DisplayName("obtain the 'store as' type for a given column type")
    void obtainStoreAsType() {
        var type = mapping.typeOf(Message.class);

        assertThat(type).isEqualTo(STRING);
    }

    @Nested
    @DisplayName("store identically")
    class StoreIdentically {

        @Test
        @DisplayName("`String` column values")
        void stringColumns() {
            var str = "some-string";
            assertConverts(str, str);
        }

        @Test
        @DisplayName("`int` column values")
        void integerColumns() {
            var num = 42;
            assertConverts(num, num);
        }

        @Test
        @DisplayName("`long` column values")
        void longColumns() {
            var num = 42L;
            assertConverts(num, num);
        }

        @Test
        @DisplayName("`boolean` column values")
        void booleanColumns() {
            var theBoolean = false;
            assertConverts(theBoolean, theBoolean);
        }

        @Test
        @DisplayName("`null` column values")
        void nullColumns() {
            var result = mapping.ofNull()
                                .applyTo(null);
            assertThat(result).isNull();
        }
    }

    @Nested
    @DisplayName("throw `IAE` because of unsupported type")
    class ThrowUnsupportedType {

        @Test
        @DisplayName("for `float` columns")
        void forFloatColumns() {
            assertThrows(IllegalArgumentException.class, mapping::ofFloat);
        }

        @Test
        @DisplayName("for `double` columns")
        void forDoubleColumns() {
            assertThrows(IllegalArgumentException.class, mapping::ofDouble);
        }
    }

    @Test
    @DisplayName("store `ByteString` as byte array")
    void storeByteStringAsByteArray() {
        var bytes = new byte[]{(byte) 1, (byte) 2, (byte) 3};
        var byteString = ByteString.copyFrom(bytes);
        assertConverts(byteString, bytes);
    }

    @Test
    @DisplayName("store enum as its ordinal")
    void storeEnumAsOrdinal() {
        var status = Status.CREATED;
        assertConverts(status, status.getNumber());
    }

    @Test
    @DisplayName("store `Message` as JSON string")
    void storeMessageAsJson() {
        var id = StgProjectId
                .newBuilder()
                .setId("the-project-ID")
                .build();
        var project = StgProject
                .newBuilder()
                .setId(id)
                .build();
        assertConverts(project, toCompactJson(project));
    }

    @Test
    @DisplayName("store `Timestamp` as nanos")
    void storeTimestampAsNanos() {
        var timestamp = Timestamp
                .newBuilder()
                .setSeconds(432342)
                .setNanos(12312)
                .build();
        assertConverts(timestamp, toNanos(timestamp));
    }

    @Test
    @DisplayName("store `Version` as version number")
    void storeVersionAsNumber() {
        var version = Versions.zero();
        assertConverts(version, version.getNumber());
    }

    private void assertConverts(Object object, Object expected) {
        var result = mapping.of(object.getClass())
                            .applyTo(object);
        assertThat(result).isEqualTo(expected);
    }
}
