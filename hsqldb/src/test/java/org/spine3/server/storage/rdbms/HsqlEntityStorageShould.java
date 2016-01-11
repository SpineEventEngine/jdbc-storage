/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.rdbms;

import com.google.protobuf.StringValue;
import com.zaxxer.hikari.HikariConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.spine3.server.storage.EntityStorageShould;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class HsqlEntityStorageShould extends EntityStorageShould {

    /**
     * The URL of in-memory database.
     */
    private static final String DB_URL = "jdbc:hsqldb:mem:entitytests";

    private static final HsqlDb DATABASE = newHsqlDb();

    private static final HsqlEntityStorage<String, StringValue> STORAGE = newStorage(DATABASE);

    public HsqlEntityStorageShould() {
        super(STORAGE);
    }

    private static HsqlDb newHsqlDb() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        return HsqlDb.newInstance(config);
    }

    private static HsqlEntityStorage<String, StringValue> newStorage(HsqlDb db) {
        return HsqlEntityStorage.newInstance(db, StringValue.getDescriptor());
    }

    @After
    public void tearDownTest() {
        try {
            STORAGE.clear();
        } catch (DatabaseException e) {
            // NOP
        }
    }

    @AfterClass
    public static void tearDownClass() {
        STORAGE.close();
    }

    @Test
    public void close_itself() {
        final HsqlEntityStorage<String, StringValue> storage = newStorage(newHsqlDb());
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
        final String id = "testid";
        final StringValue entity = StringValue.newBuilder().setValue("testvalue").build();

        STORAGE.write(id, entity);
        STORAGE.clear();

        final StringValue actual = STORAGE.read(id);
        assertNull(actual);
    }
}
