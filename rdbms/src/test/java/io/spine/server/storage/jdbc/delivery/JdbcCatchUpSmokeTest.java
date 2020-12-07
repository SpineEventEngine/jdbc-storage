/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.delivery;

import io.spine.base.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.delivery.CatchUpTest;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.testing.SlowTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_1_4;

/**
 * Smoke tests on {@link io.spine.server.delivery.CatchUp CatchUp} functionality running
 * on top of JDBC-accessible storage.
 *
 * <p>The tests are extremely slow, so only a tiny portion of the original {@link CatchUpTest}
 * is launched.
 */
@SlowTest
@DisplayName("JDBC-backed `CatchUp` should ")
class JdbcCatchUpSmokeTest extends CatchUpTest {

    private StorageFactory factory;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        DataSourceWrapper source = whichIsStoredInMemory(newUuid());
        factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(source)
                .setTypeMapping(H2_1_4)
                .build();
        ServerEnvironment
                .when(Tests.class)
                .use(factory);
    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();
        try {
            factory.close();
        } catch (Exception e) {
            throw new IllegalStateException("Error closing the storage factory", e);
        }
    }

    @Test
    @Disabled
    @Override
    public void withNanosByIds() throws InterruptedException {
        super.withNanosByIds();
    }

    @Test
    @Disabled
    @Override
    public void withMillisByIds() throws InterruptedException {
        super.withMillisByIds();
    }

    @Test
    @Disabled
    @Override
    public void withMillisAllInOrder() throws InterruptedException {
        super.withMillisAllInOrder();
    }
}
