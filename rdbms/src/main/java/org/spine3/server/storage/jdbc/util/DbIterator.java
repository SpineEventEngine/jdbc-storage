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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import static org.spine3.server.storage.jdbc.util.Serializer.readDeserializedRecord;

/**
 * TODO:2015-12-08:alexander.litus: docs
 *
 * @author Alexander Litus
 */
@Internal
public class DbIterator<Record extends Message> implements Iterator<Record>, AutoCloseable {

    private final ResultSet resultSet;
    private final PreparedStatement statement;
    private final String columnName;
    private final Descriptor descriptor;
    private boolean isHasNextCalledBeforeNext = false;

    public DbIterator(PreparedStatement statement, String columnName, Descriptor descriptor)
            throws DatabaseException {
        try {
            this.resultSet = statement.executeQuery();
            this.statement = statement;
            this.columnName = columnName;
            this.descriptor = descriptor;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            final boolean hasNext = resultSet.next();
            isHasNextCalledBeforeNext = true;
            return hasNext;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
    public Record next() {
        if (!isHasNextCalledBeforeNext) {
            throw new IllegalStateException("It is required to call hasNext() before next() method.");
        }
        isHasNextCalledBeforeNext = false;

        final Record record = readDeserializedRecord(resultSet, columnName, descriptor);
        return record;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing is not supported.");
    }

    @Override
    public void close() throws DatabaseException {
        try {
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
