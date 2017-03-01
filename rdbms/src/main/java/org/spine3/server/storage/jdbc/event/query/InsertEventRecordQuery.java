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

package org.spine3.server.storage.jdbc.event.query;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.spine3.base.Event;
import org.spine3.base.EventContext;
import org.spine3.base.Stringifiers;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.TypeName;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_ID_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_TYPE_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.NANOSECONDS_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.PRODUCER_ID_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.SECONDS_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.TABLE_NAME;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * Query that inserts a new {@link Event} to the {@link EventTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class InsertEventRecordQuery extends WriteRecordQuery<String, Event> {

    private static final int PARAMETER_COUNT = QueryParameter.values().length;

    private static final String QUERY_TEMPLATE =
            INSERT_INTO + TABLE_NAME + BRACKET_OPEN +
            EVENT_ID_COL + COMMA +
            EVENT_COL + COMMA +
            EVENT_TYPE_COL + COMMA +
            PRODUCER_ID_COL + COMMA +
            SECONDS_COL + COMMA +
            NANOSECONDS_COL + BRACKET_CLOSE +
            VALUES + Sql.nPlaceholders(PARAMETER_COUNT) + SEMICOLON;

    private InsertEventRecordQuery(Builder builder) {
        super(builder);
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = connection.prepareStatement(QUERY_TEMPLATE);
        final Event event = getRecord();
        final EventContext context = event.getContext();
        final Timestamp timestamp = context.getTimestamp();
        try {
            final String eventId = getId();
            statement.setString(QueryParameter.EVENT_ID.getIndex(), eventId);

            final byte[] serializedEvent = serialize(event);
            statement.setBytes(QueryParameter.EVENT.getIndex(), serializedEvent);

            final Any eventMessageAny = event.getMessage();
            final Message eventMessage = AnyPacker.unpack(eventMessageAny);
            final String eventType = TypeName.of(eventMessage);
            statement.setString(QueryParameter.EVENT_TYPE.getIndex(), eventType);

            final Any producerIdAny = context.getProducerId();
            final Message producerId = AnyPacker.unpack(producerIdAny);
            final String producerIdString = Stringifiers.idToString(producerId);
            statement.setString(QueryParameter.PRODUCER_ID.getIndex(), producerIdString);

            final long seconds = timestamp.getSeconds();
            statement.setLong(QueryParameter.SECONDS.getIndex(), seconds);

            final int nanos = timestamp.getNanos();
            statement.setInt(QueryParameter.NANOS.getIndex(), nanos);
        } catch (SQLException e) {
            getLogger().error("Failed to build statement", e);
            throw new DatabaseException(e);
        }
        return statement;
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends WriteRecordQuery.Builder<Builder, InsertEventRecordQuery, String, Event> {

        @Override
        public InsertEventRecordQuery build() {
            return new InsertEventRecordQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }

    private enum QueryParameter {

        EVENT_ID(1),
        EVENT(2),
        EVENT_TYPE(3),
        PRODUCER_ID(4),
        SECONDS(5),
        NANOS(6);

        private final int index;

        QueryParameter(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }
}
