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

package org.spine3.server.storage.jdbc.projection;

import org.junit.Test;
import org.spine3.server.projection.Projection;
import org.spine3.server.projection.ProjectionStorage;
import org.spine3.server.projection.ProjectionStorageShould;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.entity.JdbcRecordStorage;
import org.spine3.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import org.spine3.server.storage.jdbc.projection.query.ProjectionStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.projection.Project;

import static org.spine3.base.Identifiers.newUuid;

/**
 * @author Alexander Litus
 */
public class JdbcProjectionStorageShould extends ProjectionStorageShould<String> {

    @Override
    protected ProjectionStorage<String> getStorage() {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "projectionStorageTests");
        final Class<TestProjection> projectionClass = TestProjection.class;
        final JdbcRecordStorage<String> entityStorage = JdbcRecordStorage.newInstance(
                dataSource,
                false,
                new RecordStorageQueryFactory<>(
                        dataSource,
                        projectionClass));
        return JdbcProjectionStorage.newInstance(entityStorage, false,
                                                 new ProjectionStorageQueryFactory<>(dataSource,
                                                                                     projectionClass));
    }

    @Override
    protected String newId() {
        return newUuid();
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final ProjectionStorage<?> storage = getStorage();
        storage.close();
        storage.close();
    }

    private static class TestProjection extends Projection<String, Project> {

        private TestProjection(String id) {
            super(id);
        }
    }
}
