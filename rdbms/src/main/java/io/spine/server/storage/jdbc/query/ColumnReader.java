/*
 * Copyright 2018, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import io.spine.annotation.Internal;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A reader of the designated column value in the current row of the {@link ResultSet}.
 *
 * @param <R>
 *         the type of read operation result
 */
@Internal
public abstract class ColumnReader<R> {

    private final String columnName;

    /**
     * Creates a new {@code ColumnReader} for the specified column.
     *
     * @param columnName
     *         the name of the column to read
     */
    protected ColumnReader(String columnName) {
        this.columnName = columnName;
    }

    String columnName() {
        return columnName;
    }

    /**
     * Reads the value of the designated column in the current row of the {@link ResultSet}.
     *
     * @param resultSet
     *         the result set to read the column value from
     * @return the read operation result
     * @throws SQLException
     *         if an error occurs during the read operation, e.g. the column doesn't exist or the
     *         {@code ResultSet} is closed
     */
    public abstract R readValue(ResultSet resultSet) throws SQLException;
}
