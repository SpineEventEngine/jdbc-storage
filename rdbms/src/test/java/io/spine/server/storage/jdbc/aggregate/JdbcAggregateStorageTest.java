/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.aggregate;

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.aggregate.AggregateStorageTest;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;

@DisplayName("RDBMS-backed `AggregateStorage` should")
class JdbcAggregateStorageTest extends AggregateStorageTest {

    private JdbcStorageFactory factory;

    @BeforeEach
    void setUp() {
        factory = JdbcStorageFactory.newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setTypeMapping(H2_2_1)
                .build();
        ServerEnvironment
                .when(Tests.class)
                .use(factory);
    }

    @AfterEach
    void tearDown() {
        ServerEnvironment.instance().reset();
        if(factory != null) {
            factory.close();
        }
    }
}
