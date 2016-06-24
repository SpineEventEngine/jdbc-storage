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

package org.spine3.server.storage.jdbc.query;


import com.google.protobuf.Message;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class Write<M extends Message> extends Abstract {

    protected Write(Builder<? extends Builder, ? extends Write> builder) {
        super(builder);
    }

    /**
     * Executes a write query.
     */
    @SuppressWarnings("ReturnOfNull")
    @Override
    public M execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.dataSource.getConnection(false)) {
            try (PreparedStatement statement = prepareStatement(connection)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                // logError(e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
        return null;
    }

    protected abstract static class Builder<B extends Builder<B, Q>, Q extends Write> extends Abstract.Builder<B, Q>{}

}
