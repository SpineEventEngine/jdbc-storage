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
import com.google.protobuf.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.Internal;
import org.spine3.base.CommandStatus;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;

/**
 * A query which obtains a {@link Message} by an ID.
 *
 * @author Alexander Litus
 */
@Internal
public class SelectByStatusQuery {

    private final String query;

    private final CommandStatus status;

    /**
     * Creates a new query instance.
     *
     * @param query SQL select query which selects a message by an ID (must have one ID parameter to set)
     */
    protected SelectByStatusQuery(String query, CommandStatus status) {
        this.query = query;
        this.status = status;
    }


    public PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = connection.prepareStatement(query);
        try {
            statement.setString(1, status.toString());
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }
}
