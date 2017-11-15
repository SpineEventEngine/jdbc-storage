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

import io.spine.server.storage.jdbc.TypeMapping.Builder;
import org.junit.Test;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.ID;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.TypeMappings.mySql;

/**
 * @author Dmytro Grankin
 */
public class TypeMappingShould {

    @Test(expected = IllegalArgumentException.class)
    public void throw_exception_on_attempt_to_map_ID_type() {
        TypeMapping.newBuilder()
                   .add(ID, ID.name());
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_if_required_types_not_mapped() {
        final Builder builder = TypeMapping.newBuilder()
                                           .add(BOOLEAN, BOOLEAN.name())
                                           .add(STRING, STRING.name());
        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void throw_ISE_if_requested_type_has_no_mapping() {
        mySql().getTypeName(ID);
    }
}
