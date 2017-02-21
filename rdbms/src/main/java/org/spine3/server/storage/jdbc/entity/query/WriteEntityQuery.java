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

import org.spine3.server.entity.status.EntityStatus;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Dmytro Dashenkov.
 */
public class WriteEntityQuery<I> extends WriteRecordQuery<I, EntityStorageRecord> {

    protected static final int RECORD_COL_POSITION = 1;

    protected static final int ARCHIVED_COL_POSITION = 2;

    protected static final int DELETED_COL_POSITION = 3;

    protected static final int ID_COL_POSITION = 4;

    protected WriteEntityQuery(
            Builder<? extends Builder, ? extends WriteRecordQuery, I, EntityStorageRecord> builder) {
        super(builder);
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        final EntityStorageRecord record = getRecord();
        final EntityStatus status = record.getEntityStatus();
        final boolean archived = status.getArchived();
        final boolean deleted = status.getDeleted();
        try {
            statement.setBoolean(ARCHIVED_COL_POSITION, archived);
            statement.setBoolean(DELETED_COL_POSITION, deleted);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }
}
