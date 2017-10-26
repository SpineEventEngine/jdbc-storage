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

package io.spine.server.storage.jdbc.query;

import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * An abstract base for {@linkplain WriteQuery write queries}.
 *
 * @author Alexander Litus
 */
abstract class AbstractWriteQuery extends AbstractQuery implements WriteQuery {

    protected AbstractWriteQuery(Builder<? extends Builder, ? extends WriteQuery> builder) {
        super(builder);
    }

    @Override
    public void execute() {
        try (ConnectionWrapper connection = getConnection(false)) {
            try (PreparedStatement statement = prepareStatement(connection)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                getLogger().error("Failed to execute write operation.", e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public abstract static class Builder<B extends Builder<B, Q>, Q extends AbstractWriteQuery>
            extends AbstractQuery.Builder<B, Q> {
    }
}