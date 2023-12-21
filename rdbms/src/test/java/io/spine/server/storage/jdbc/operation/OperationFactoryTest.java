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

package io.spine.server.storage.jdbc.operation;

import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.storage.Storage;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.PredefinedMapping;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.engine.PredefinedEngine;
import io.spine.server.storage.jdbc.operation.given.OperationFactoryTestEnv;
import io.spine.server.storage.jdbc.operation.given.OperationFactoryTestEnv.OverridingAllOpFactory;
import io.spine.server.storage.jdbc.operation.given.OperationFactoryTestEnv.TestOperationFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.deliveryContextSpec;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.inboxMessageSpec;
import static io.spine.server.storage.jdbc.operation.given.OperationFactoryTestEnv.OVERRIDDEN_LABEL;
import static io.spine.server.storage.jdbc.operation.given.OperationFactoryTestEnv.imStorageFactoryBuilder;
import static io.spine.server.storage.jdbc.operation.given.OperationFactoryTestEnv.randomHandleCommandMessage;
import static org.junit.jupiter.api.Assertions.fail;

@DisplayName("With `OperationFactory` it should be possible")
final class OperationFactoryTest {

    @Test
    @DisplayName("use custom operation instead of a default one")
    void useCustomOperations() {
        var factory = imStorageFactoryBuilder()
                .useOperationFactory(TestOperationFactory::new)
                .build();
        Storage<InboxMessageId, InboxMessage> storage =
                factory.createRecordStorage(deliveryContextSpec(), inboxMessageSpec());

        var message = randomHandleCommandMessage();
        assertThat(message.getLabel())
                .isNotEqualTo(OVERRIDDEN_LABEL);
        storage.write(message.getId(), message);

        var actual = storage.read(message.getId());
        assertThat(actual)
                .isPresent();
        assertThat(actual.get()
                         .getLabel())
                .isEqualTo(OVERRIDDEN_LABEL);
    }

    /**
     * This test instantiates the {@linkplain OverridingAllOpFactory custom operation factory},
     * which overrides every available operation,
     * and uses the descendants of the framework-provided operations,
     * to ensure they are extensible.
     *
     * @see OverridingAllOpFactory
     */
    @Test
    @DisplayName("extend existing operations, " +
            "and use them in corresponding methods in `OperationFactory`")
    void useOperationsByExtending() {
        var dataSource = inMemoryDataSource();
        var typeMapping = PredefinedMapping.select(dataSource);
        var factory = new OverridingAllOpFactory(dataSource, typeMapping);
        assertThat(factory)
                .isNotNull();
    }

    @Test
    @DisplayName("override a hard-coded RDBMS engine via `protected` ctor of `OperationFactory`")
    void customizeEngine() {
        var dataSource = inMemoryDataSource();
        var typeMapping = PredefinedMapping.select(dataSource);
        var postgres = PredefinedEngine.Postgres;
        var postgresFactory = new OperationFactory(dataSource, typeMapping, postgres);
        assertThat(postgresFactory.engine())
                .isEqualTo(postgres);

        try {
            var constructor = OperationFactory.class
                    .getDeclaredConstructor(DataSourceWrapper.class,
                                            TypeMapping.class,
                                            PredefinedEngine.class);
            assertThat(constructor)
                    .isNotNull();
        } catch (NoSuchMethodException e) {
            fail("No expected `protected OperationFactory(" +
                         "DataSourceWrapper, TypeMapping, DetectedEngine)` ctor was found.");
        }
    }

    private static DataSourceWrapper inMemoryDataSource() {
        return whichIsStoredInMemory(OperationFactoryTestEnv.class.getName());
    }
}
