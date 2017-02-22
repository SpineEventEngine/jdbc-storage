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

package org.spine3.server.storage.jdbc.entity;

import com.google.common.base.Optional;
import com.google.protobuf.Message;
import org.junit.Test;
import org.spine3.server.entity.AbstractEntity;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.RecordStorageShould;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.storage.Project;
import org.spine3.test.storage.ProjectId;
import org.spine3.testdata.Sample;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.spine3.base.Identifiers.newUuid;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcRecordStorageShould extends RecordStorageShould<String, JdbcRecordStorage<String>> {

    @Override
    protected JdbcRecordStorage<String> getStorage() {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "entityStorageTests");
        return JdbcRecordStorage.newInstance(dataSource,
                                             false,
                                             new RecordStorageQueryFactory<>(
                                                     dataSource,
                                                     TestEntityWithStringId.class),
                                             Project.getDescriptor());
    }

    @Override
    protected String newId() {
        return newUuid();
    }

    @Test
    public void close_itself_and_throw_exception_on_read() {
        final JdbcRecordStorage<String> storage = getStorage();
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
        final JdbcRecordStorage<String> storage = getStorage();
        final String id = newUuid();
        final EntityRecord record = newStorageRecord();
        storage.writeRecord(id, record);
        storage.clear();

        final Optional<EntityRecord> actual = storage.readRecord(id);
        assertNotNull(actual);
        assertFalse(actual.isPresent());
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final RecordStorage<?> storage = getStorage();
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

    private static class TestEntityWithStringId extends AbstractEntity<String, Project> {

        private TestEntityWithStringId(String id) {
            super(id);
        }
    }
}
