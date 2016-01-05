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

package org.spine3.server.storage.hsqldb;

import com.google.protobuf.StringValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.spine3.server.storage.EntityStorageShould;

import static org.spine3.server.storage.hsqldb.HsqlEntityStorage.*;

/**
 * @author Alexander Litus
 */
public class HsqlEntityStorageShould extends EntityStorageShould {

    /**
     * The URL of in-memory database.
     */
    private static final String DB_URL = "jdbc:hsqldb:mem:entitytests";

    private static final HsqlDb DATABASE = HsqlDb.newInstance(DB_URL);

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE " + ENTITIES + '(' +
                ID + " VARCHAR(999)," +
                ENTITY + " BLOB," +
                "PRIMARY KEY(" + ID + ')' +
            ");";

    private static final String DROP_TABLE_SQL = "DROP TABLE " + ENTITIES;

    private static final String SHUTDOWN_SQL = "SHUTDOWN";

    public HsqlEntityStorageShould() {
        super(newStorage());
    }

    private static HsqlEntityStorage<String, StringValue> newStorage() {
        return HsqlEntityStorage.newInstance(DATABASE, StringValue.getDescriptor());
    }

    @Before
    public void setUpTest() {
        DATABASE.execute(CREATE_TABLE_SQL);
    }

    @After
    public void tearDownTest() {
        DATABASE.execute(DROP_TABLE_SQL);
    }

    @AfterClass
    public static void tearDownClass() {
        DATABASE.execute(SHUTDOWN_SQL);
        DATABASE.close();
    }
}
