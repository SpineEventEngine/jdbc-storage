/*
 * Copyright 2026 CodeMatters, Lda.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package io.spine.server.storage.jdbc.delivery;

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.delivery.DeliveryTest;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.testing.SlowTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_4;

/**
 * Smoke tests on {@link Delivery} functionality running on top of JDBC-accessible storage.
 *
 * <p>The tests are extremely slow, so only a tiny portion of the original {@link DeliveryTest}
 * is launched.
 */
@SlowTest
@DisplayName("JDBC-backed `Delivery` should ")
class JdbcDeliverySmokeTest extends DeliveryTest {

    private StorageFactory factory;

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        var source = whichIsStoredInMemory(newUuid());
        factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(source)
                .setTypeMapping(H2_2_4)
                .build();
        ServerEnvironment
                .under(Tests.class)
                .use(factory);
    }

    @AfterEach
    @Override
    public void tearDown() {
        super.tearDown();
        try {
            factory.close();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot close the storage factory", e);
        }
    }

    @Test
    @DisplayName("deliver messages via multiple shards to multiple targets in a multi-threaded env")
    @Override
    public void manyTargets_manyShards_manyThreads() {
        super.manyTargets_manyShards_manyThreads();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void markDelivered() {
        super.markDelivered();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void singleTarget_singleShard_manyThreads() {
        super.singleTarget_singleShard_manyThreads();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void manyTargets_singleShard_manyThreads() {
        super.manyTargets_singleShard_manyThreads();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void singleTarget_manyShards_manyThreads() {
        super.singleTarget_manyShards_manyThreads();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void singleTarget_manyShards_singleThread() {
        super.singleTarget_manyShards_singleThread();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void singleTarget_singleShard_singleThread() {
        super.singleTarget_singleShard_singleThread();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void manyTargets_singleShard_singleThread() {
        super.manyTargets_singleShard_singleThread();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void manyTargets_manyShards_singleThread() {
        super.manyTargets_manyShards_singleThread();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void withCustomStrategy() {
        super.withCustomStrategy();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void calculateStats() {
        super.calculateStats();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void returnOptionalEmptyIfPicked() {
        super.returnOptionalEmptyIfPicked();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void notifyDeliveryMonitorOfDeliveryCompletion() {
        super.notifyDeliveryMonitorOfDeliveryCompletion();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void deliverInBatch() {
        super.deliverInBatch();
    }

    @Test
    @Disabled(SmokeTesting.DISABLED_REASON)
    @Override
    public void deliverMessagesInOrderOfEmission() throws InterruptedException {
        super.deliverMessagesInOrderOfEmission();
    }
}
