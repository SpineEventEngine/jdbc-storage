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

package io.spine.server.storage.jdbc.projection;

import io.spine.server.entity.Entity;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.projection.ProjectionStorageShould;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.builder.StorageBuilder;
import io.spine.server.storage.jdbc.entity.JdbcRecordStorage;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.test.projection.Project;
import io.spine.test.storage.ProjectId;
import io.spine.validate.ValidatingBuilder;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * @author Alexander Litus
 */
public class JdbcProjectionStorageShould extends ProjectionStorageShould {

    @Override
    protected ProjectionStorage<ProjectId> getStorage(Class<? extends Entity> entityClass) {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "projectionStorageTests");
        final Class<TestProjection> projectionClass = TestProjection.class;
        final JdbcRecordStorage<ProjectId> entityStorage =
                JdbcRecordStorage.<ProjectId>newBuilder()
                                 .setDataSource(dataSource)
                                 .setMultitenant(false)
                                 .setEntityClass(projectionClass)
                                 .setColumnTypeRegistry(JdbcTypeRegistryFactory.defaultInstance())
                                 .build();
        final ProjectionStorage<ProjectId> storage =
                JdbcProjectionStorage.<ProjectId>newBuilder()
                                     .setRecordStorage(entityStorage)
                                     .setDataSource(dataSource)
                                     .setMultitenant(false)
                                     .setProjectionClass(projectionClass)
                                     .build();
        return storage;
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final ProjectionStorage<?> storage = getStorage(TestProjection.class);
        storage.close();
        storage.close();
    }

    @Test
    public void accept_datasource_in_builder_event_though_not_uses_it() {
        final StorageBuilder<?, ?> builder =
                JdbcProjectionStorage.newBuilder()
                                     .setDataSource(GivenDataSource.withoutSuperpowers());
        assertNotNull(builder);
    }

    private static class TestProjection extends Projection<ProjectId, Project, ValidatingBuilder<Project, Project.Builder>> {

        private TestProjection(ProjectId id) {
            super(id);
        }
    }
}
