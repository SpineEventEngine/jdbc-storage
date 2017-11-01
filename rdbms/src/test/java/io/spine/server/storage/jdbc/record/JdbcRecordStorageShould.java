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

package io.spine.server.storage.jdbc.record;

import com.google.common.base.Optional;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.client.ColumnFilter;
import io.spine.client.CompositeColumnFilter;
import io.spine.client.EntityFilters;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageShould;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv;
import io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.TestEntityWithStringId;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.testdata.Sample;
import org.junit.Test;

import static io.spine.Identifier.newUuid;
import static io.spine.client.ColumnFilters.gt;
import static io.spine.client.ColumnFilters.lt;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Alexander Litus
 */
public class JdbcRecordStorageShould
        extends RecordStorageShould<String, JdbcRecordStorage<String>> {

    @Test
    public void close_itself_and_throw_exception_on_read() {
        final JdbcRecordStorage<String> storage = getStorage(TestEntityWithStringId.class);
        storage.close();
        try {
            storage.readRecord("any-id");
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Storage must close itself.");
    }

    @Test
    public void clear_itself() {
        final JdbcRecordStorage<String> storage = getStorage(TestEntityWithStringId.class);
        final String id = newUuid();
        final EntityRecord entityRecord = newStorageRecord();

        final EntityRecordWithColumns record = EntityRecordWithColumns.of(entityRecord);
        storage.writeRecord(id, record);
        storage.clear();

        final Optional<EntityRecord> actual = storage.readRecord(id);
        assertNotNull(actual);
        assertFalse(actual.isPresent());
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final RecordStorage<?> storage = getStorage(TestEntityWithStringId.class);
        storage.close();
        storage.close();
    }

    @Test
    public void use_column_names_for_storing() {
        final JdbcRecordStorage<String> storage = getStorage(TestEntityWithStringId.class);
        final int entityColumnIndex = RecordTable.StandardColumn.values().length;
        final String customColumnName = storage.getTable()
                                               .getTableColumns()
                                               .get(entityColumnIndex)
                                               .name();
        assertEquals(JdbcRecordStorageTestEnv.COLUMN_NAME_FOR_STORING, customColumnName);
    }

    @Test
    public void read_by_composite_filter_with_column_filters_for_same_column() {
        final JdbcRecordStorage<String> storage = getStorage(TestEntityWithStringId.class);
        final String columnName = "value";
        final ColumnFilter lessThan = lt(columnName, -5);
        final ColumnFilter greaterThan = gt(columnName, 5);
        final CompositeColumnFilter columnFilter = CompositeColumnFilter.newBuilder()
                                                                        .addFilter(lessThan)
                                                                        .addFilter(greaterThan)
                                                                        .setOperator(ALL)
                                                                        .build();
        final EntityFilters entityFilters = EntityFilters.newBuilder()
                                                         .addFilter(columnFilter)
                                                         .build();
        final EntityQuery<String> query = EntityQueries.from(entityFilters,
                                                             TestEntityWithStringId.class);
        storage.readAll(query, FieldMask.getDefaultInstance());
    }

    @Override
    protected JdbcRecordStorage<String> getStorage(Class<? extends Entity> cls) {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "entityStorageTests");
        @SuppressWarnings("unchecked") // Test invariant.
        final Class<? extends Entity<String, ?>> entityClass =
                (Class<? extends Entity<String, ?>>) cls;
        final JdbcRecordStorage<String> storage =
                JdbcRecordStorage.<String>newBuilder()
                        .setDataSource(dataSource)
                        .setEntityClass(entityClass)
                        .setMultitenant(false)
                        .setColumnTypeRegistry(JdbcTypeRegistryFactory.defaultInstance())
                        .build();
        return storage;
    }

    @Override
    protected String newId() {
        return newUuid();
    }

    @Override
    protected Message newState(String id) {
        final Project.Builder builder = Sample.builderForType(Project.class);
        builder.setId(ProjectId.newBuilder()
                               .setId(id));
        return builder.build();
    }

    @Override
    protected Class<? extends TestCounterEntity> getTestEntityClass() {
        return JdbcRecordStorageTestEnv.TestCounterEntityJdbc.class;
    }
}
