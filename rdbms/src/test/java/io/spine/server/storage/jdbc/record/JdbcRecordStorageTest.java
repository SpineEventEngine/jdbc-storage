/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.protobuf.Message;
import io.spine.base.Identifier;
import io.spine.client.CompositeFilter;
import io.spine.client.Filter;
import io.spine.client.ResponseFormat;
import io.spine.client.TargetFilters;
import io.spine.protobuf.AnyPacker;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageTest;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.type.DefaultJdbcStorageRules;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.testdata.Sample;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.client.CompositeFilter.CompositeOperator.ALL;
import static io.spine.client.Filters.gt;
import static io.spine.client.Filters.lt;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_1_4;
import static io.spine.test.storage.Project.Status.DONE;
import static io.spine.testing.Tests.nullRef;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({
        "InnerClassMayBeStatic", "ClassCanBeStatic", /* JUnit nested classes cannot be static. */
        "DuplicateStringLiteralInspection" /* Common test display names. */
})
@DisplayName("JdbcRecordStorage should")
class JdbcRecordStorageTest extends RecordStorageTest<JdbcRecordStorage<ProjectId>> {

    @Test
    @DisplayName("clear itself")
    void clearItself() {
        JdbcRecordStorage<ProjectId> storage = storage();
        ProjectId id = newId();
        EntityRecord entityRecord = newStorageRecord();

        EntityRecordWithColumns record = EntityRecordWithColumns.create(entityRecord, storage);
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
        ProjectId id = ProjectId
                .newBuilder()
                .setId(newUuid())
                .build();
        TestCounterEntity entity = new TestCounterEntity(id);
        entity.assignStatus(DONE);
        EntityRecord record = EntityRecord
                .newBuilder()
                .setEntityId(Identifier.pack(id))
                .setState(pack(entity.state()))
                .setVersion(entity.version())
                .setLifecycleFlags(entity.lifecycleFlags())
                .build();
        EntityRecordWithColumns recordWithColumns =
                EntityRecordWithColumns.create(record, entity, storage);
        storage.write(id, recordWithColumns);

        String columnName = "project_status_value";
        Filter lessThan = lt(columnName, 4);
        Filter greaterThan = gt(columnName, 2);
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
        ProjectId nextId = AnyPacker.unpack(next.getEntityId(), ProjectId.class);

        assertThat(nextId).isEqualTo(id);

        close(storage);
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
        @DisplayName("column storage rules")
        void columnStorageRules() {
            DefaultJdbcStorageRules storageRules = nullRef();
            assertThrows(NullPointerException.class,
                         () -> JdbcRecordStorage.newBuilder()
                                                .setColumnStorageRules(storageRules));
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
                        .setColumnStorageRules(new DefaultJdbcStorageRules())
                        .setTypeMapping(H2_1_4)
                        .build();
        return storage;
    }

    @Override
    protected Message newState(ProjectId id) {
        Project.Builder builder = Sample.builderForType(Project.class);
        builder.setId(id);
        return builder.build();
    }

    @Override
    protected Class<? extends TestCounterEntity> getTestEntityClass() {
        return TestCounterEntity.class;
    }
}
