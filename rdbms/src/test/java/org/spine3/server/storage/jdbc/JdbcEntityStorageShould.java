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

import com.google.protobuf.StringValue;
import com.google.protobuf.util.TimeUtil;
import com.zaxxer.hikari.HikariConfig;
import org.junit.After;
import org.junit.Test;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.EntityStorageShould;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.spine3.protobuf.Messages.toAny;
import static org.spine3.server.util.Identifiers.newUuid;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcEntityStorageShould extends EntityStorageShould {

    /**
     * The URL of in-memory HyperSQL DB.
     */
    private static final String DB_URL = "jdbc:hsqldb:mem:entitytests";

    private final JdbcEntityStorage<String> storage = newStorage();

    @Override
    protected EntityStorage<String> getStorage() {
        return storage;
    }

    private static JdbcEntityStorage<String> newStorage() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        // not setting username and password is OK for in-memory database
        final DataSourceWrapper dataSource = HikariDataSourceWrapper.newInstance(config);
        return JdbcEntityStorage.newInstance(dataSource, JdbcStorageFactoryShould.TestEntity.class);
    }

    @After
    public void tearDownTest() {
        storage.close();
    }

    @Test
    public void close_itself() {
        final JdbcEntityStorage<String> storage = newStorage();
        storage.close();
        try {
            storage.readInternal("any-id");
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Storage should close itself.");
    }

    @Test
    public void clear_itself() {
        final String id = newUuid();
        final EntityStorageRecord record = newEntityRecord();
        storage.writeInternal(id, record);
        storage.clear();

        final EntityStorageRecord actual = storage.readInternal(id);
        assertNull(actual);
    }

    private static EntityStorageRecord newEntityRecord() {
        final StringValue stringValue = StringValue.newBuilder().setValue(newUuid()).build();
        final EntityStorageRecord.Builder builder = EntityStorageRecord.newBuilder()
                .setState(toAny(stringValue))
                .setVersion(5) // set any non-default value
                .setWhenModified(TimeUtil.getCurrentTime());
        return builder.build();
    }
}
