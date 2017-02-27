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

package org.spine3.server.storage.jdbc.entity.query;

import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Dmytro Dashenkov.
 */
public class WriteEntityQuery<I> extends WriteRecordQuery<I, EntityRecord> {

    protected WriteEntityQuery(
            Builder<? extends Builder, ? extends WriteRecordQuery, I, EntityRecord> builder) {
        super(builder);
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        final EntityRecord record = getRecord();
        final Visibility status = record.getVisibility();
        final boolean archived = status.getArchived();
        final boolean deleted = status.getDeleted();
        try {
            statement.setBoolean(QueryParameter.ARCHIVED.index, archived);
            statement.setBoolean(QueryParameter.DELETED.index, deleted);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }

    protected enum QueryParameter {

        RECORD(1),
        ARCHIVED(2),
        DELETED(3),
        ID(4);

        public final int index;

        QueryParameter(int index) {
            this.index = index;
        }
    }
}
