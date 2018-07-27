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

package io.spine.server.storage.jdbc.type;

import io.spine.core.Version;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.storage.jdbc.type.given.JdbcTypeRegistryFactoryTestEnv.CustomType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.jdbc.type.given.JdbcTypeRegistryFactoryTestEnv.columnWithType;
import static io.spine.testing.DisplayNames.HAVE_PARAMETERLESS_CTOR;
import static io.spine.testing.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("JdbcTypeRegistryFactory should")
class JdbcTypeRegistryFactoryTest {

    @Test
    @DisplayName(HAVE_PARAMETERLESS_CTOR)
    void haveUtilityConstructor() {
        assertHasPrivateParameterlessCtor(JdbcTypeRegistryFactory.class);
    }

    @Test
    @DisplayName("provide default type registry for required types")
    void provideDefaultRegistry() {
        ColumnTypeRegistry<?> registry = JdbcTypeRegistryFactory.defaultInstance();
        assertNotNull(registry);
        assertNotNull(registry.get(columnWithType(Version.class)));
        assertNotNull(registry.get(columnWithType(boolean.class)));
    }

    @Test
    @DisplayName("provide builder for extending defaults")
    void provideBuilder() {
        ColumnTypeRegistry<?> registry =
                JdbcTypeRegistryFactory.predefinedValuesAnd()
                                       .put(String.class, CustomType.INSTANCE)
                                       .build();
        assertNotNull(registry);
        assertNotNull(registry.get(columnWithType(Version.class)));
        assertNotNull(registry.get(columnWithType(boolean.class)));
        assertEquals(registry.get(columnWithType(String.class)), CustomType.INSTANCE);
    }
}
