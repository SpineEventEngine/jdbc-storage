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

package org.spine3.server.storage.jdbc.projection.query;

import com.google.protobuf.Timestamp;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.SqlQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.projection.query.ProjectionTable.NANOS_COL;
import static org.spine3.server.storage.jdbc.projection.query.ProjectionTable.SECONDS_COL;
import static org.spine3.validate.Validate.isDefault;

/**
 * Query that selects timestamp from the {@link ProjectionTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectTimestampQuery extends SqlQuery {
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE = "SELECT " +
            SECONDS_COL + ", " +
            NANOS_COL + " FROM %s ;";

    private SelectTimestampQuery(Builder builder) {
        super(builder);
    }

    @Nullable
    public Timestamp execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.getConnection(true);
             PreparedStatement statement = prepareStatement(connection);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final long seconds = resultSet.getLong(SECONDS_COL);
            final int nanos = resultSet.getInt(NANOS_COL);
            final Timestamp time = Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
            if (isDefault(time)) {
                return null;
            }
            return time;
        } catch (SQLException e) {
            this.getLogger().error("Failed to read last event time.", e);
            throw new DatabaseException(e);
        }
    }

    public static Builder newBuilder(String tableName) {
        final Builder builder = new Builder();
        builder.setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends SqlQuery.Builder<Builder, SelectTimestampQuery> {

        @Override
        public SelectTimestampQuery build() {
            return new SelectTimestampQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
