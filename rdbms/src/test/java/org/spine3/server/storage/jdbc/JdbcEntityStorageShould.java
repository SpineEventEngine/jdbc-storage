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
import org.junit.AfterClass;
import org.junit.Test;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.EntityStorageShould;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.spine3.protobuf.Messages.toAny;
import static org.spine3.util.Identifiers.newUuid;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcEntityStorageShould extends EntityStorageShould {

    /**
     * The URL of in-memory HyperSQL DB.
     */
    private static final String DB_URL = "jdbc:hsqldb:mem:entitytests";

    private static final DataSourceWrapper DATA_SOURCE = newDataSource();

    private static final JdbcEntityStorage<String> STORAGE = newStorage(DATA_SOURCE);

    public JdbcEntityStorageShould() {
        super(STORAGE);
    }

    private static DataSourceWrapper newDataSource() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        // username and password are not required for such a database
        return HikariDataSourceWrapper.newInstance(config);
    }

    private static JdbcEntityStorage<String> newStorage(DataSourceWrapper db) {
        return JdbcEntityStorage.newInstance(db, JdbcStorageFactoryShould.TestEntity.class);
    }

    @After
    public void tearDownTest() {
        STORAGE.clear();
    }

    @AfterClass
    public static void tearDownClass() {
        STORAGE.close();
    }

    @Test
    public void close_itself() {
        final JdbcEntityStorage<String> storage = newStorage(newDataSource());
        storage.close();
        try {
            storage.read("any-id");
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Storage should close itself.");
    }

    @Test
    public void clear_itself() {
        final String idString = newUuid();
        final EntityStorageRecord record = newEntityRecord(newUuid(), idString);
        STORAGE.write(record);
        STORAGE.clear();

        final EntityStorageRecord actual = STORAGE.read(idString);
        assertNull(actual);
    }

    private static EntityStorageRecord newEntityRecord(String value, String id) {
        final EntityStorageRecord.Id recordId = EntityStorageRecord.Id.newBuilder().setStringValue(id).build();
        final StringValue stringValue = StringValue.newBuilder().setValue(newUuid()).build();
        final EntityStorageRecord.Builder builder = EntityStorageRecord.newBuilder()
                .setState(toAny(stringValue))
                .setId(recordId)
                .setVersion(5) // set any non-default value
                .setWhenModified(TimeUtil.getCurrentTime());
        return builder.build();
    }
}
