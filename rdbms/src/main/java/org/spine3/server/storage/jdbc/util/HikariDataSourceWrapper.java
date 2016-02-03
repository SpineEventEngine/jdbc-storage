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

package org.spine3.server.storage.jdbc.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;

import javax.sql.DataSource;

/**
 * The wrapper for {@link HikariDataSource}.
 *
 * @see <a href="https://github.com/brettwooldridge/HikariCP">HikariCP connection pool</a>
 * @author Alexander Litus
 */
@Internal
public class HikariDataSourceWrapper extends DataSourceWrapper {

    private final HikariDataSource dataSource;

    /**
     * Creates a new instance with the specified configuration.
     *
     * <p>Please see
     * <a href="https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby">HikariCP configuration</a>
     * for more info.
     *
     * @param config the config used to create {@link HikariDataSource}.
     */
    public static HikariDataSourceWrapper newInstance(HikariConfig config) {
        return new HikariDataSourceWrapper(config);
    }

    protected HikariDataSourceWrapper(HikariConfig config) {
        dataSource = new HikariDataSource(config);
    }

    @Override
    public void close() throws DatabaseException {
        try {
            dataSource.close();
        } catch (RuntimeException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }
}
