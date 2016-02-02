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

package org.spine3.server.storage.jdbc;

import com.zaxxer.hikari.HikariConfig;
import org.junit.After;
import org.junit.Test;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageShould;
import org.spine3.test.project.ProjectId;

import static org.junit.Assert.fail;
import static org.spine3.testdata.TestAggregateIdFactory.createProjectId;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcAggregateStorageShould extends AggregateStorageShould {

    /**
     * The URL of the in-memory HyperSQL DB.
     */
    private static final String DB_URL = "jdbc:hsqldb:mem:aggregateStorageTests";

    private final JdbcAggregateStorage<ProjectId> storage = newStorage();

    @Override
    protected AggregateStorage<ProjectId> getStorage() {
        return storage;
    }

    private static JdbcAggregateStorage<ProjectId> newStorage() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        // not setting username and password is OK for in-memory database
        final DataSourceWrapper dataSource = HikariDataSourceWrapper.newInstance(config);
        return JdbcAggregateStorage.newInstance(dataSource, JdbcStorageFactoryShould.TestEntity.class);
    }

    @After
    public void tearDownTest() {
        storage.close();
    }

    @Test
    public void close_itself() {
        final JdbcAggregateStorage<ProjectId> storage = newStorage();
        storage.close();
        try {
            storage.historyBackward(createProjectId("anyId"));
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Aggregate storage should close itself.");
    }
}
