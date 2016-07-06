/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

import com.google.protobuf.Message;
import org.junit.Test;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageShould;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.project.Project;
import org.spine3.test.project.ProjectId;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.spine3.server.storage.jdbc.JdbcStorageFactoryShould.newInMemoryDataSource;
import static org.spine3.testdata.TestAggregateIdFactory.newProjectId;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcAggregateStorageShould extends AggregateStorageShould {

    @Override
    protected AggregateStorage<ProjectId> getStorage() {
        final JdbcAggregateStorage<ProjectId> storage = getStorage(TestAggregateWithMessageId.class);
        return storage;
    }

    @Override
    protected <I> JdbcAggregateStorage<I> getStorage(Class<? extends Aggregate<I, ? extends Message, ?>> aggregateClass) {
        final DataSourceWrapper dataSource = newInMemoryDataSource("aggregateStorageTests");
        return JdbcAggregateStorage.newInstance(dataSource, false, new AggregateStorageQueryFactory<>(dataSource, aggregateClass));
    }

    @Test
    public void close_itself() {
        final JdbcAggregateStorage<ProjectId> storage = getStorage(TestAggregateWithMessageId.class);
        storage.close();
        try {
            storage.historyBackward(newProjectId("anyId"));
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Aggregate storage should close itself.");
    }

    private static class TestAggregateWithMessageId extends Aggregate<ProjectId, Project, Project.Builder> {
        private TestAggregateWithMessageId(ProjectId id) {
            super(id);
        }
    }
}
