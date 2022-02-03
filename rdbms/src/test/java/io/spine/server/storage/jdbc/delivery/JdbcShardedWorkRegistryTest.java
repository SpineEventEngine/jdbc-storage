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

package io.spine.server.storage.jdbc.delivery;

import com.google.common.testing.NullPointerTester;
import io.spine.base.Identifier;
import io.spine.server.NodeId;
import io.spine.server.delivery.DeliveryStrategy;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.delivery.ShardSessionRecord;
import io.spine.server.delivery.ShardedWorkRegistry;
import io.spine.server.delivery.ShardedWorkRegistryTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.ContextSpec.singleTenant;
import static io.spine.server.delivery.DeliveryStrategy.newIndex;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;

@DisplayName("`JdbcShardedWorkRegistry` should")
class JdbcShardedWorkRegistryTest extends ShardedWorkRegistryTest {

    private JdbcShardedWorkRegistry registry;

    @BeforeEach
    void setUp() {
        var factory = newFactory();
        var context = singleTenant(JdbcShardedWorkRegistryTest.class.getName());
        registry = new JdbcShardedWorkRegistry(factory, context);
    }

    @Override
    protected ShardedWorkRegistry registry() {
        return registry;
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .setDefault(NodeId.class, newNode())
                .setDefault(ShardIndex.class, newIndex(4, 5))
                .setDefault(ShardSessionRecord.class, ShardSessionRecord.getDefaultInstance())
                .testAllPublicInstanceMethods(registry);
    }

    private static NodeId newNode() {
        return NodeId.newBuilder()
                     .setValue(Identifier.newUuid())
                     .vBuild();
    }
}
