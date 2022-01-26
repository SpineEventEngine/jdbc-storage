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

package io.spine.server.storage.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.server.storage.jdbc.GivenDataSource.whichHoldsMetadata;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.PredefinedMapping.POSTGRESQL_10_1;
import static io.spine.server.storage.jdbc.PredefinedMapping.select;
import static io.spine.testing.TestValues.nullRef;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`PredefinedMapping` should")
class PredefinedMappingTest {

    private static final PredefinedMapping mapping = POSTGRESQL_10_1;

    @Test
    @DisplayName("throw ISE if requested type has no mapping")
    void throwOnNoMapping() {
        Type notMappedType = nullRef();
        assertThrows(IllegalStateException.class, () -> MYSQL_5_7.typeNameFor(notMappedType));
    }

    @Test
    @DisplayName("be selected by database product name and major version")
    void selectTypeMapping() {
        var dataSource = whichHoldsMetadata(mapping.getDatabaseProductName(),
                                            mapping.getMajorVersion(),
                                            mapping.getMinorVersion());
        assertThat(select(dataSource))
                .isEqualTo(mapping);
    }

    @Test
    @DisplayName("not be selected if major versions are different")
    void notSelectForDifferentVersion() {
        var newMajorVersion = mapping.getMajorVersion() + 1;
        var dataSource = whichHoldsMetadata(mapping.getDatabaseProductName(),
                                            newMajorVersion,
                                            mapping.getMinorVersion());
        assertThat(select(dataSource))
                .isNotEqualTo(mapping);
    }
}
