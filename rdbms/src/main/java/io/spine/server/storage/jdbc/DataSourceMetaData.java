/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.server.storage.jdbc;

import io.spine.annotation.Internal;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * A metadata of the {@linkplain javax.sql.DataSource data source}.
 */
@Internal
public interface DataSourceMetaData {

    /**
     * Obtains the database product name.
     */
    String productName();

    /**
     * Obtains the database major version number.
     */
    int majorVersion();

    /**
     * Obtains the database minor version number.
     */
    int minorVersion();

    /**
     * Wraps a given database {@linkplain DatabaseMetaData metadata}.
     *
     * @throws DatabaseException
     *         in case something went wrong when interacting with database
     */
    static DataSourceMetaData of(DatabaseMetaData metaData) throws DatabaseException {
        try {
            var productName = metaData.getDatabaseProductName();
            var majorVersion = metaData.getDatabaseMajorVersion();
            var minorVersion = metaData.getDatabaseMinorVersion();

            return new DataSourceMetaData() {
                @Override
                public String productName() {
                    return productName;
                }

                @Override
                public int majorVersion() {
                    return majorVersion;
                }

                @Override
                public int minorVersion() {
                    return minorVersion;
                }
            };
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
