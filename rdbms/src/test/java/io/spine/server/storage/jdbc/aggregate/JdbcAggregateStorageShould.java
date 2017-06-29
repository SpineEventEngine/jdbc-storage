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

package io.spine.server.storage.jdbc.aggregate;

import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageShould;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.test.aggregate.Project;
import io.spine.test.aggregate.ProjectId;
import io.spine.validate.ValidatingBuilder;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcAggregateStorageShould extends AggregateStorageShould {

    @Override
    protected AggregateStorage<ProjectId> getStorage(Class<? extends Entity> aClass) {

        return null;
    }

    @Override
    protected <I> JdbcAggregateStorage<I> getStorage(
            Class<? extends I> idClass,
            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        final DataSourceWrapper dataSource =
                GivenDataSource.whichIsStoredInMemory("aggregateStorageTests");
        final JdbcAggregateStorage<I> storage =
                JdbcAggregateStorage.<I>newBuilder()
                        .setMultitenant(false)
                        .setDataSource(dataSource)
                        .setAggregateClass(aggregateClass)
                        .build();
        return storage;
    }

    @Test
    public void throw_exception_if_try_to_use_closed_storage() {
        final JdbcAggregateStorage<ProjectId> storage = getStorage(ProjectId.class,
                                                                   TestAggregateWithMessageId.class);
        storage.close();
        try {
            storage.historyBackward(ProjectId.getDefaultInstance());
        } catch (DatabaseException expected) {
            // expected exception because the storage is closed
            return;
        }
        fail("Aggregate storage should close itself.");
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final AggregateStorage<?> storage = getStorage(ProjectId.class,
                                                       TestAggregateWithMessageId.class);
        storage.close();
        storage.close();
    }

    private static class TestAggregateWithMessageId extends Aggregate<ProjectId,
            Project,
            ValidatingBuilder<Project, Project.Builder>> {
        private TestAggregateWithMessageId(ProjectId id) {
            super(id);
        }
    }
}
