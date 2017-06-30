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

package io.spine.server.storage.jdbc.entity;

import com.google.common.base.Optional;
import com.google.protobuf.Message;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.RecordStorageShould;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.testdata.Sample;
import org.junit.Test;

import static io.spine.Identifier.newUuid;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcRecordStorageShould
        extends RecordStorageShould<String, JdbcRecordStorage<String>> {

    @Override
    protected JdbcRecordStorage<String> getStorage(Class<? extends Entity> cls) {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "entityStorageTests");
        final JdbcRecordStorage<String> storage =
                JdbcRecordStorage.<String>newBuilder()
                                 .setDataSource(dataSource)
                                 .setEntityClass(cls)
                                 .setMultitenant(false)
                                 .setColumnTypeRegistry(JdbcTypeRegistryFactory.defaultInstance())
                                 .build();
        return storage;
    }

    @Override
    protected String newId() {
        return newUuid();
    }

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
        final String columnValue = "i'm a value";
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

    static class TestCounterEntityJdbc extends TestCounterEntity<String> {
        protected TestCounterEntityJdbc(String id) {
            super(id);
        }
    }

    private static class TestEntityWithStringId extends AbstractEntity<String, Project> {

        private TestEntityWithStringId(String id) {
            super(id);
        }
    }
}
