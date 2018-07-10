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

package io.spine.server.storage.jdbc;

import io.spine.type.TypeName;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Grankin
 */
public class TypeMappingBuilderShould {

    @Test
    @DisplayName("override type names")
    void overrideTypeNames() {
        final Type type = BYTE_ARRAY;
        final String originalName = "original";
        final String nameReplacement = "replacement";
        final TypeMapping mapping = TypeMappingBuilder.basicBuilder()
                                                      .add(type, originalName)
                                                      .add(type, nameReplacement)
                                                      .build();
        final TypeName resultingName = mapping.typeNameFor(type);
        assertEquals(nameReplacement, resultingName.value());
    }

    @Test(expected = IllegalArgumentException.class)
    @DisplayName("not allow empty type names")
    void notAllowEmptyTypeNames() {
        final TypeMappingBuilder builder = TypeMappingBuilder.basicBuilder();
        builder.add(BYTE_ARRAY, "");
    }

    @Test(expected = IllegalStateException.class)
    @DisplayName("throw exception if not all types mapped")
    void throwExceptionIfNotAllTypesMapped() {
        final TypeMappingBuilder builder = new TypeMappingBuilder();
        builder.build();
    }
}
