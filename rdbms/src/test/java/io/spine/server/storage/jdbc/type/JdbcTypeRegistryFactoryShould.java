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

package io.spine.server.storage.jdbc.type;

import io.spine.core.Version;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.query.Parameters;
import org.junit.Test;

import static io.spine.test.Tests.assertHasPrivateParameterlessCtor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class JdbcTypeRegistryFactoryShould {

    @Test
    public void have_private_util_ctor() {
        assertHasPrivateParameterlessCtor(JdbcTypeRegistryFactory.class);
    }

    @Test
    public void provide_default_type_registry_for_required_types() {
        final ColumnTypeRegistry<?> registry = JdbcTypeRegistryFactory.defaultInstance();
        assertNotNull(registry);
        assertNotNull(registry.get(columnWithType(Version.class)));
        assertNotNull(registry.get(columnWithType(boolean.class)));
    }

    @Test
    public void provide_builder_for_extending_defaults() {
        final ColumnTypeRegistry<?> registry =
                JdbcTypeRegistryFactory.predefinedValuesAnd()
                                       .put(String.class, CustomType.INSTANCE)
                                       .build();
        assertNotNull(registry);
        assertNotNull(registry.get(columnWithType(Version.class)));
        assertNotNull(registry.get(columnWithType(boolean.class)));
        assertEquals(registry.get(columnWithType(String.class)),
                     CustomType.INSTANCE);
    }

    private static EntityColumn columnWithType(Class<?> cls) {
        final EntityColumn column = mock(EntityColumn.class);
        when(column.getType()).thenReturn(cls);
        return column;
    }

    private enum CustomType implements JdbcColumnType<String, String> {

        INSTANCE;

        @Override
        public Sql.Type getSqlType() {
            return Sql.Type.VARCHAR_255;
        }

        @Override
        public String convertColumnValue(String fieldValue) {
            return fieldValue;
        }

        @Override
        public void setColumnValue(Parameters.Builder storageRecord, String value,
                                   Integer columnIdentifier) {
            // NoOp (as for tests)
        }

        @Override
        public void setNull(Parameters.Builder storageRecord, Integer columnIdentifier) {
            // NoOp (as for tests)
        }
    }
}
