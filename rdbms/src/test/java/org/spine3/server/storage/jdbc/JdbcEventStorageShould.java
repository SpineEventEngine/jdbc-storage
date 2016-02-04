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
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageShould;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.HikariDataSourceWrapper;

import static org.spine3.server.storage.jdbc.JdbcStorageFactoryShould.getInMemoryDbUrl;

/**
 * @author Alexander Litus
 */
public class JdbcEventStorageShould extends EventStorageShould {

    private static final String DB_URL = getInMemoryDbUrl("eventStorageTests");

    private final JdbcEventStorage storage = newStorage();

    @Override
    protected EventStorage getStorage() {
        return storage;
    }

    private static JdbcEventStorage newStorage() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        // not setting username and password is OK for in-memory database
        final DataSourceWrapper dataSource = HikariDataSourceWrapper.newInstance(config);
        return JdbcEventStorage.newInstance(dataSource);
    }

    @After
    public void tearDownTest() {
        storage.clear();// TODO:2016-02-04:alexander.litus: find out why we have to clear
        storage.close();
    }
}
