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

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idReader;

/**
 * An iterator over the IDs of a table.
 *
 * @author Dmytro Dashenkov
 */
class IndexIterator<I> extends DbIterator<I> {

    private final Class<I> idType;

    /**
     * Creates a new iterator instance.
     *  @param resultSet  a result set of IDs (will be closed on a {@link #close()})
     * @param columnName a name of a serialized storage record column
     * @param idType
     */
    IndexIterator(ResultSet resultSet, String columnName, Class<I> idType) {
        super(resultSet, columnName);
        this.idType = idType;
    }

    @Override
    protected I readResult() throws SQLException {
        ColumnReader<I> columnReader = idReader(getColumnName(), idType);
        I result = columnReader.read(getResultSet());
        return result;
    }
}
