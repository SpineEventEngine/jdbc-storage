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

package io.spine.server.storage.jdbc.aggregate;

import com.google.common.base.Optional;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageVisibilityHandlingTest;
import io.spine.server.entity.LifecycleFlags;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.aggregate.given.JdbcAggregateStorageVisibilityHandlingTestEnv.TestAggregate;
import io.spine.test.aggregate.ProjectId;
import io.spine.testdata.Sample;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("JdbcAggregateStorage, when saving aggregate with lifecycle flags, should")
class JdbcAggregateStorageVisibilityHandlingTest
        extends AggregateStorageVisibilityHandlingTest {

    @Override
    protected AggregateStorage<ProjectId> getAggregateStorage(
            Class<? extends Aggregate<ProjectId, ?, ?>> aggregateClass) {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "aggregateStorageStatusHandlingTests");
        final JdbcAggregateStorage<ProjectId> storage =
                JdbcAggregateStorage.<ProjectId>newBuilder()
                        .setMultitenant(false)
                        .setAggregateClass(TestAggregate.class)
                        .setDataSource(dataSource)
                        .setTypeMapping(MYSQL_5_7)
                        .build();
        return storage;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent") // We do check.
    @Test
    @DisplayName("update entity visibility")
    void updateEntityVisibility() {
        final ProjectId id = Sample.messageOfType(ProjectId.class);
        final LifecycleFlags archived = LifecycleFlags.newBuilder()
                                              .setArchived(true)
                                              .build();
        final JdbcAggregateStorage<ProjectId> storage =
                (JdbcAggregateStorage<ProjectId>) getAggregateStorage(TestAggregate.class);
        storage.writeLifecycleFlags(id, archived);

        final Optional<LifecycleFlags> actualArchived = storage.readLifecycleFlags(id);
        assertTrue(actualArchived.isPresent());
        assertEquals(archived, actualArchived.get());

        final LifecycleFlags archivedAndDeleted = LifecycleFlags.newBuilder()
                                                        .setArchived(true)
                                                        .setDeleted(true)
                                                        .build();
        storage.writeLifecycleFlags(id, archivedAndDeleted);

        final Optional<LifecycleFlags> actualArchivedAndDeleted = storage.readLifecycleFlags(id);
        assertTrue(actualArchivedAndDeleted.isPresent());
        assertEquals(archivedAndDeleted, actualArchivedAndDeleted.get());
    }
}