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

package org.spine3.server.storage.jdbc.event;

import org.junit.Test;
import org.spine3.server.event.EventStorage;
import org.spine3.server.event.EventStorageShould;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.event.query.EventStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

/**
 * @author Alexander Litus
 */
public class JdbcEventStorageShould extends EventStorageShould {

    @Override
    protected EventStorage getStorage() {
        final DataSourceWrapper dataSource =
                GivenDataSource.whichIsStoredInMemory("eventStorageTests");
        return JdbcEventStorage.newInstance(dataSource, false,
                                            new EventStorageQueryFactory(dataSource));
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final EventStorage storage = getStorage();
        storage.close();
        storage.close();
    }
}
