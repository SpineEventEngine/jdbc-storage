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

import io.spine.server.storage.jdbc.BaseMapping.Builder;
import org.junit.Test;

import static io.spine.server.storage.jdbc.StandardMapping.MYSQL_5;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.test.Tests.nullRef;
import static org.junit.Assert.assertEquals;

/**
 * @author Dmytro Grankin
 */
public class BaseMappingShould {

    @Test
    public void allow_override_type_names_during_building() {
        final Type type = BYTE_ARRAY;
        final String originalName = "original";
        final String nameReplacement = "replacement";
        final TypeMapping mapping = BaseMapping.baseBuilder()
                                               .add(type, originalName)
                                               .add(type, nameReplacement)
                                               .build();
        final String resultingName = mapping.getTypeName(type);
        assertEquals(nameReplacement, resultingName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_allow_empty_type_names() {
        final Builder builder = BaseMapping.baseBuilder();
        builder.add(BYTE_ARRAY, "");
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_if_not_all_types_mapped() {
        final Builder builder = new BaseMapping.Builder();
        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void throw_ISE_if_requested_type_has_no_mapping() {
        final Type notMappedType = nullRef();
        MYSQL_5.getTypeName(notMappedType);
    }
}
