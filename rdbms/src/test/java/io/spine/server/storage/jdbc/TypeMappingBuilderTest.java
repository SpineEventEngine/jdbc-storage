/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import io.spine.type.TypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Dmytro Grankin
 */
@DisplayName("TypeMappingBuilder should")
class TypeMappingBuilderTest {

    @Test
    @DisplayName("override type names")
    void overrideTypeNames() {
        Type type = BYTE_ARRAY;
        String originalName = "original";
        String nameReplacement = "replacement";
        TypeMapping mapping = TypeMappingBuilder.basicBuilder()
                                                .add(type, originalName)
                                                .add(type, nameReplacement)
                                                .build();
        TypeName resultingName = mapping.typeNameFor(type);
        assertEquals(nameReplacement, resultingName.value());
    }

    @Test
    @DisplayName("not allow empty type names")
    void rejectEmptyTypeNames() {
        TypeMappingBuilder builder = TypeMappingBuilder.basicBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.add(BYTE_ARRAY, ""));
    }

    @Test
    @DisplayName("throw ISE if not all types are mapped")
    void throwIfNotAllTypesMapped() {
        TypeMappingBuilder builder = new TypeMappingBuilder();
        assertThrows(IllegalStateException.class, builder::build);
    }
}
