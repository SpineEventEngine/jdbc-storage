/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.spine.base.EntityState;
import io.spine.client.CompositeFilter;
import io.spine.client.Filter;
import io.spine.client.OrderBy;
import io.spine.client.ResponseFormat;
import io.spine.client.TargetFilters;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageTest;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv;
import io.spine.server.storage.jdbc.type.DefaultJdbcColumnMapping;
import io.spine.test.storage.Project;
import io.spine.test.storage.Project.Status;
import io.spine.test.storage.ProjectId;
import io.spine.testdata.Sample;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static io.spine.client.CompositeFilter.CompositeOperator.ALL;
import static io.spine.client.Filters.gt;
import static io.spine.client.Filters.lt;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.asEntityRecord;
import static io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.ascendingByStatusValue;
import static io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.descendingByStatusValue;
import static io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.queryAllBeforeCancelled;
import static io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.projectId;
import static io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.statusValueColumn;
import static io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.unpackProjectId;
import static io.spine.test.storage.Project.Status.CANCELLED;
import static io.spine.test.storage.Project.Status.CREATED;
import static io.spine.test.storage.Project.Status.DONE;
import static io.spine.test.storage.Project.Status.STARTED;
import static io.spine.testing.Tests.nullRef;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({
        "InnerClassMayBeStatic", "ClassCanBeStatic", /* JUnit nested classes cannot be static. */
        "DuplicateStringLiteralInspection" /* Common test display names. */
})
@DisplayName("`JdbcRecordStorage` should")
public class JdbcRecordStorageTest extends RecordStorageTest<JdbcRecordStorage<ProjectId>> {

    @Test
    @DisplayName("clear itself")
    void clearItself() {
        JdbcRecordStorage<ProjectId> storage = storage();
        ProjectId id = newId();
        EntityRecord entityRecord = newStorageRecord();

        EntityRecordWithColumns record = EntityRecordWithColumns.of(entityRecord);
        storage.writeRecord(id, record);
        storage.clear();

        Optional<EntityRecord> actual = storage.readRecord(id);
        assertNotNull(actual);
        assertFalse(actual.isPresent());
        close(storage);
    }

    @Test
    @DisplayName("throw ISE when closing twice")
    void throwOnClosingTwice() {
        RecordStorage<?> storage = storage();
        storage.close();
        assertThrows(IllegalStateException.class, storage::close);
    }

    @Test
    @DisplayName("read by composite filter with column filters for same column")
    void readByCompositeFilter() {
        JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
        Status status = DONE;
        ProjectId id = writeProjectWithStatus(storage, status);

        Filter lessThan = lt(statusValueColumn(), 4);
        Filter greaterThan = gt(statusValueColumn(), 2);
        CompositeFilter columnFilter = CompositeFilter
                .newBuilder()
                .addFilter(lessThan)
                .addFilter(greaterThan)
                .setOperator(ALL)
                .build();
        TargetFilters filters = TargetFilters
                .newBuilder()
                .addFilter(columnFilter)
                .build();
        EntityQuery<ProjectId> query = EntityQueries.from(filters, storage);
        Iterator<EntityRecord> resultIterator =
                storage.readAll(query, ResponseFormat.getDefaultInstance());

        assertThat(resultIterator.hasNext())
                .isTrue();

        EntityRecord next = resultIterator.next();
        ProjectId nextId = unpackProjectId(next);

        assertThat(nextId).isEqualTo(id);

        close(storage);
    }

    @CanIgnoreReturnValue
    private static ProjectId writeProjectWithStatus(JdbcRecordStorage<ProjectId> storage,
                                                    Status status) {
        ProjectId id = projectId();
        TestCounterEntity entity = new TestCounterEntity(id);

        entity.assignStatus(status);
        EntityRecord record = asEntityRecord(id, entity);
        EntityRecordWithColumns recordWithColumns =
                EntityRecordWithColumns.create(record, entity, storage);
        storage.write(id, recordWithColumns);
        return id;
    }

    @Nested
    @DisplayName("allow to order records when specifying the column for")
    class OrderingAndLimit {

        @Test
        @DisplayName("the ascending sorting")
        void ascending() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            ProjectId done = writeProjectWithStatus(storage, DONE);
            ProjectId created = writeProjectWithStatus(storage, CREATED);
            ProjectId started = writeProjectWithStatus(storage, STARTED);
            ProjectId cancelled = writeProjectWithStatus(storage, CANCELLED);

            OrderBy ordering = ascendingByStatusValue();
            ImmutableList<ProjectId> actualIds = readIds(storage, ordering);
            assertThat(actualIds)
                    .containsExactly(created, started, done, cancelled)
                    .inOrder();
            close(storage);
        }

        @Test
        @DisplayName("the ascending sorting with limit")
        void ascendingWithLimit() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            writeProjectWithStatus(storage, DONE);
            ProjectId created = writeProjectWithStatus(storage, CREATED);
            writeProjectWithStatus(storage, STARTED);
            writeProjectWithStatus(storage, CANCELLED);

            OrderBy ordering = ascendingByStatusValue();
            int returnJustOne = 1;
            ImmutableList<ProjectId> actualIds = readIds(storage, ordering, returnJustOne);
            assertThat(actualIds)
                    .containsExactly(created)
                    .inOrder();
            close(storage);
        }

        @Test
        @DisplayName("the ascending sorting, filtering the records by some column value")
        void ascendingWithFiltering() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            ProjectId done = writeProjectWithStatus(storage, DONE);
            ProjectId created = writeProjectWithStatus(storage, CREATED);
            ProjectId started = writeProjectWithStatus(storage, STARTED);
            writeProjectWithStatus(storage, CANCELLED);

            EntityQuery<ProjectId> query = queryAllBeforeCancelled(storage);
            OrderBy ordering = ascendingByStatusValue();
            ImmutableList<ProjectId> actualIds = readIds(storage, query, ordering);
            assertThat(actualIds)
                    .containsExactly(created, started, done)
                    .inOrder();
            close(storage);
        }

        @Test
        @DisplayName("the ascending sorting with limit, filtering the records by some column value")
        void ascendingWithFilteringAndLimit() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            writeProjectWithStatus(storage, DONE);
            ProjectId created = writeProjectWithStatus(storage, CREATED);
            ProjectId started = writeProjectWithStatus(storage, STARTED);
            writeProjectWithStatus(storage, CANCELLED);

            EntityQuery<ProjectId> query = queryAllBeforeCancelled(storage);
            OrderBy ordering = ascendingByStatusValue();
            int returnOnlyTwo = 2;
            ImmutableList<ProjectId> actualIds = readIds(storage, query, ordering, returnOnlyTwo);
            assertThat(actualIds)
                    .containsExactly(created, started)
                    .inOrder();
            close(storage);
        }

        @Test
        @DisplayName("the descending sorting")
        void descending() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            ProjectId done = writeProjectWithStatus(storage, DONE);
            ProjectId created = writeProjectWithStatus(storage, CREATED);
            ProjectId started = writeProjectWithStatus(storage, STARTED);
            ProjectId cancelled = writeProjectWithStatus(storage, CANCELLED);

            OrderBy ordering = descendingByStatusValue();
            ImmutableList<ProjectId> actualIds = readIds(storage, ordering);
            assertThat(actualIds)
                    .containsExactly(cancelled, done, started, created)
                    .inOrder();
            close(storage);
        }

        @Test
        @DisplayName("the descending sorting, filtering the records by some column value")
        void descendingWithFiltering() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            ProjectId done = writeProjectWithStatus(storage, DONE);
            ProjectId created = writeProjectWithStatus(storage, CREATED);
            ProjectId started = writeProjectWithStatus(storage, STARTED);
            writeProjectWithStatus(storage, CANCELLED);

            EntityQuery<ProjectId> query = queryAllBeforeCancelled(storage);
            OrderBy ordering = descendingByStatusValue();
            ImmutableList<ProjectId> actualIds = readIds(storage, query, ordering);
            assertThat(actualIds)
                    .containsExactly(done, started, created)
                    .inOrder();
            close(storage);
        }

        @Test
        @DisplayName("the descending sorting with limit, filtering the records by a column value")
        void descendingWithFilteringAndLimit() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            ProjectId done = writeProjectWithStatus(storage, DONE);
            writeProjectWithStatus(storage, CREATED);
            writeProjectWithStatus(storage, STARTED);
            writeProjectWithStatus(storage, CANCELLED);

            EntityQuery<ProjectId> query = queryAllBeforeCancelled(storage);
            OrderBy ordering = descendingByStatusValue();
            int returnJustOne = 1;
            ImmutableList<ProjectId> actualIds = readIds(storage, query, ordering, returnJustOne);
            assertThat(actualIds)
                    .containsExactly(done)
                    .inOrder();
            close(storage);
        }

        @Test
        @DisplayName("the descending sorting with limit")
        void descendingWithLimit() {
            JdbcRecordStorage<ProjectId> storage = newStorage(TestCounterEntity.class);
            ProjectId done = writeProjectWithStatus(storage, DONE);
            writeProjectWithStatus(storage, CREATED);
            ProjectId started = writeProjectWithStatus(storage, STARTED);
            ProjectId cancelled = writeProjectWithStatus(storage, CANCELLED);

            OrderBy ordering = descendingByStatusValue();
            int returnTopThree = 3;
            ImmutableList<ProjectId> actualIds = readIds(storage, ordering, returnTopThree);
            assertThat(actualIds)
                    .containsExactly(cancelled, done, started)
                    .inOrder();
            close(storage);
        }
    }

    @Nested
    @DisplayName("require non-null")
    class RequireNonNull {

        @Test
        @DisplayName("entity class")
        void entityClass() {
            Class<? extends Entity<Object, ?>> nullEntityCls = nullRef();
            assertThrows(NullPointerException.class,
                         () -> JdbcRecordStorage.newBuilder()
                                                .setEntityClass(nullEntityCls));
        }


        @Test
        @DisplayName("column mapping")
        @SuppressWarnings({"CheckReturnValue",
                "ResultOfMethodCallIgnored" /* Method called to throw exception. */})
        void columnMapping() {
            DefaultJdbcColumnMapping columnMapping = nullRef();
            assertThrows(NullPointerException.class,
                         () -> JdbcRecordStorage.newBuilder()
                                                .setColumnMapping(columnMapping));
        }
    }

    @Override
    protected JdbcRecordStorage<ProjectId> newStorage(Class<? extends Entity<?, ?>> cls) {
        DataSourceWrapper dataSource = whichIsStoredInMemory("entityStorageTests");
        @SuppressWarnings("unchecked") // Test invariant.
                Class<? extends Entity<ProjectId, ?>> entityClass =
                (Class<? extends Entity<ProjectId, ?>>) cls;
        JdbcRecordStorage<ProjectId> storage =
                JdbcRecordStorage.<ProjectId>newBuilder()
                        .setDataSource(dataSource)
                        .setEntityClass(entityClass)
                        .setMultitenant(false)
                        .setColumnMapping(new DefaultJdbcColumnMapping())
                        .setTypeMapping(H2_2_1)
                        .build();
        return storage;
    }

    @Override
    protected EntityState newState(ProjectId id) {
        Project.Builder builder = Sample.builderForType(Project.class);
        builder.setId(id);
        return builder.build();
    }

    @Override
    protected Class<? extends TestCounterEntity> getTestEntityClass() {
        return TestCounterEntity.class;
    }

    private static ImmutableList<ProjectId>
    readIds(JdbcRecordStorage<ProjectId> storage, OrderBy ordering) {
        return readIds(storage, ordering, null);
    }

    private static ImmutableList<ProjectId> readIds(JdbcRecordStorage<ProjectId> storage,
                                                    EntityQuery<ProjectId> query,
                                                    OrderBy ordering) {
        return readIds(storage, query, ordering, null);
    }

    private static ImmutableList<ProjectId>
    readIds(JdbcRecordStorage<ProjectId> storage, OrderBy ordering, @Nullable Integer limit) {
        ResponseFormat.Builder formatBuilder = ResponseFormat.newBuilder()
                                                             .setOrderBy(ordering);
        if(limit != null) {
            formatBuilder.setLimit(limit);
        }
        ResponseFormat format = formatBuilder.vBuild();
        Iterator<EntityRecord> resultIterator = storage.readAll(format);
        ImmutableList<EntityRecord> actualRecords = ImmutableList.copyOf(resultIterator);
        ImmutableList<ProjectId> actualIds =
                actualRecords.stream()
                             .map(JdbcRecordStorageTestEnv::unpackProjectId)
                             .collect(toImmutableList());
        return actualIds;
    }

    private static ImmutableList<ProjectId>
    readIds(JdbcRecordStorage<ProjectId> storage, EntityQuery<ProjectId> query,
            OrderBy ordering, @Nullable Integer limit) {
        ResponseFormat.Builder formatBuilder = ResponseFormat.newBuilder()
                                                             .setOrderBy(ordering);
        if(limit != null) {
            formatBuilder.setLimit(limit);
        }
        ResponseFormat format = formatBuilder.vBuild();
        Iterator<EntityRecord> resultIterator = storage.readAllRecords(query, format);
        ImmutableList<EntityRecord> actualRecords = ImmutableList.copyOf(resultIterator);
        ImmutableList<ProjectId> actualIds =
                actualRecords.stream()
                             .map(JdbcRecordStorageTestEnv::unpackProjectId)
                             .collect(toImmutableList());
        return actualIds;
    }
}
