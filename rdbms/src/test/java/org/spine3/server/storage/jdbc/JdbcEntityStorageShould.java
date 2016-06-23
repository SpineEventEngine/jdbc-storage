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

import org.junit.Test;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.EntityStorageShould;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.project.Project;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.spine3.base.Identifiers.newUuid;
import static org.spine3.server.storage.jdbc.JdbcStorageFactoryShould.newInMemoryDataSource;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcEntityStorageShould extends EntityStorageShould<String> {

    @Override
    protected EntityStorage<String> getStorage() {
        return getStorage(TestEntityWithStringId.class);
    }

    @Override
    protected <I> JdbcEntityStorage<I> getStorage(Class<? extends Entity<I, ?>> entityClass) {
        final DataSourceWrapper dataSource = newInMemoryDataSource("entityStorageTests");
        return JdbcEntityStorage.newInstance(dataSource, entityClass, false);
    }

    @Override
    protected String newId() {
        return newUuid();
    }

    @Test
    public void close_itself_and_throw_exception_on_read() {
        final JdbcEntityStorage<String> storage = getStorage(TestEntityWithStringId.class);
        storage.close();
        try {
            storage.readInternal("any-id");
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Storage must close itself.");
    }

    @Test
    public void clear_itself() {
        final JdbcEntityStorage<String> storage = getStorage(TestEntityWithStringId.class);
        final String id = newUuid();
        final EntityStorageRecord record = newStorageRecord();
        storage.writeInternal(id, record);
        storage.clear();

        final EntityStorageRecord actual = storage.readInternal(id);
        assertNull(actual);
    }

    private static class TestEntityWithStringId extends Entity<String, Project> {

        private TestEntityWithStringId(String id) {
            super(id);
        }
    }
}
