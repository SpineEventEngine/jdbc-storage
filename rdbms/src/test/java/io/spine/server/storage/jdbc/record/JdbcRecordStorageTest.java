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

package io.spine.server.storage.jdbc.record;

import com.google.common.base.Optional;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.client.ColumnFilter;
import io.spine.client.CompositeColumnFilter;
import io.spine.client.EntityFilters;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityQueries;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageTest;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv;
import io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.TestCounterEntityJdbc;
import io.spine.server.storage.jdbc.record.given.JdbcRecordStorageTestEnv.TestEntityWithStringId;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.testdata.Sample;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.base.Identifier.newUuid;
import static io.spine.client.ColumnFilters.gt;
import static io.spine.client.ColumnFilters.lt;
import static io.spine.client.CompositeColumnFilter.CompositeOperator.ALL;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.test.Tests.nullRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("JdbcRecordStorage should")
class JdbcRecordStorageTest extends RecordStorageTest<String, JdbcRecordStorage<String>> {

    @Test
    @DisplayName("clear itself")
    void clearItself() {
        final JdbcRecordStorage<String> storage = getStorage();
        final String id = newUuid();
        final EntityRecord entityRecord = newStorageRecord();

        final EntityRecordWithColumns record = EntityRecordWithColumns.of(entityRecord);
        storage.writeRecord(id, record);
        storage.clear();

        final Optional<EntityRecord> actual = storage.readRecord(id);
        assertNotNull(actual);
        assertFalse(actual.isPresent());
        close(storage);
    }

    @Test
    @DisplayName("throw ISE when closing twice")
    void throwOnClosingTwice() throws Exception {
        final RecordStorage<?> storage = getStorage();
        storage.close();
        assertThrows(IllegalStateException.class, storage::close);
    }

    @Test
    @DisplayName("use column names for storing")
    void useColumnNames() {
        final JdbcRecordStorage<String> storage = newStorage(TestEntityWithStringId.class);
        final int entityColumnIndex = RecordTable.StandardColumn.values().length;
        final String customColumnName = storage.getTable()
                                               .getTableColumns()
                                               .get(entityColumnIndex)
                                               .name();
        assertEquals(JdbcRecordStorageTestEnv.COLUMN_NAME_FOR_STORING, customColumnName);
        close(storage);
    }

    @Test
    @DisplayName("read by composite filter with column filters for same column")
    void readByCompositeFilter() {
        final JdbcRecordStorage<String> storage = newStorage(TestEntityWithStringId.class);
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
        final EntityQuery<String> query = EntityQueries.from(entityFilters, storage);
        storage.readAll(query, FieldMask.getDefaultInstance());
        close(storage);
    }

    @Test
    @DisplayName("require non-null entity class")
    void rejectNullEntityClass() {
        final Class<? extends Entity<Object, ?>> nullEntityCls = nullRef();
        assertThrows(NullPointerException.class,
                     () -> JdbcRecordStorage.newBuilder()
                                            .setEntityClass(nullEntityCls));
    }

    @Test
    @DisplayName("require non-null column type registry")
    void rejectNullColumnTypeRegistry() {
        final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> registry
                = nullRef();
        assertThrows(NullPointerException.class,
                     () -> JdbcRecordStorage.newBuilder()
                                            .setColumnTypeRegistry(registry));
    }

    @Override
    protected JdbcRecordStorage<String> newStorage(Class<? extends Entity> cls) {
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
                        .setTypeMapping(MYSQL_5_7)
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
        return TestCounterEntityJdbc.class;
    }
}