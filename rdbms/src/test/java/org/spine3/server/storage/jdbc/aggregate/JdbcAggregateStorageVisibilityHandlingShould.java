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

package org.spine3.server.storage.jdbc.aggregate;

import com.google.common.base.Optional;
import org.junit.Test;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.aggregate.AggregateStorageVisibilityHandlingShould;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.aggregate.Project;
import org.spine3.test.aggregate.ProjectId;
import org.spine3.testdata.Sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Dmytro Dashenkov.
 */
public class JdbcAggregateStorageVisibilityHandlingShould extends AggregateStorageVisibilityHandlingShould {

    @Override
    protected AggregateStorage<ProjectId> getAggregateStorage(
            Class<? extends Aggregate<ProjectId, ?, ?>> aggregateClass) {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "aggregateStorageStatusHandlingTests");
        final AggregateStorageQueryFactory<ProjectId> queryFactory =
                new AggregateStorageQueryFactory<>(dataSource, TestAggregate.class);
        final JdbcAggregateStorage<ProjectId> storage = JdbcAggregateStorage.<ProjectId>newBuilder()
                                                                 .setQueryFactory(queryFactory)
                                                                 .setMultitenant(false)
                                                                 .setAggregateClass(TestAggregate.class)
                                                                 .setDataSource(dataSource)
                                                                 .build();
        return storage;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent") // We do check
    @Test
    public void update_entity_visibility() {
        final ProjectId id = Sample.messageOfType(ProjectId.class);
        final Visibility archived = Visibility.newBuilder()
                                              .setArchived(true)
                                              .build();
        final JdbcAggregateStorage<ProjectId> storage =
                (JdbcAggregateStorage<ProjectId>) getAggregateStorage(TestAggregate.class);
        storage.writeVisibility(id, archived);

        final Optional<Visibility> actualArchived = storage.readVisibility(id);
        assertTrue(actualArchived.isPresent());
        assertEquals(archived, actualArchived.get());

        final Visibility archivedAndDeleted = Visibility.newBuilder()
                                                        .setArchived(true)
                                                        .setDeleted(true)
                                                        .build();
        storage.writeVisibility(id, archivedAndDeleted);

        final Optional<Visibility> actualArchivedAndDeleted = storage.readVisibility(id);
        assertTrue(actualArchivedAndDeleted.isPresent());
        assertEquals(archivedAndDeleted, actualArchivedAndDeleted.get());
    }

    private static class TestAggregate extends Aggregate<ProjectId, Project, Project.Builder> {

        protected TestAggregate(ProjectId id) {
            super(id);
        }
    }
}
