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

import org.spine3.Internal;
import org.spine3.base.CommandStatus;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A query which obtains a {@link org.spine3.base.Command} by an command status.
 *
 * @author Andrey Lavrov
 */
@Internal
public class SelectByStatusQuery {

    private final String query;

    private final CommandStatus status;

    /**
     * Creates a new query instance.
     *
     * @param query SQL select query which selects a Command by an command status (must have one CommandStatus parameter to set)
     */
    protected SelectByStatusQuery(String query, CommandStatus status) {
        this.query = query;
        this.status = status;
    }


    /**
     * Prepares SQL statement using the connection.
     */
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
