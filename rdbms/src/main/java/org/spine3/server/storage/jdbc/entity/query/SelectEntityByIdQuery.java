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
import org.spine3.server.storage.EntityStatusField;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.ALL_ATTRIBUTES;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.entity.query.EntityTable.ENTITY_COL;
import static org.spine3.server.storage.jdbc.entity.query.EntityTable.ID_COL;

/**
 * Query that selects {@link EntityStorageRecord} by ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectEntityByIdQuery<I> extends SelectByIdQuery<I, EntityStorageRecord> {

    private static final String QUERY_TEMPLATE = SELECT.toString() + ALL_ATTRIBUTES + FROM + " %s" + WHERE
            + ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    public SelectEntityByIdQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setIdIndexInQuery(1)
                .setQuery(format(QUERY_TEMPLATE, tableName))
                .setMessageColumnName(ENTITY_COL)
                .setMessageDescriptor(EntityStorageRecord.getDescriptor());
        return builder;
    }

    @Nullable
    @Override
    protected EntityStorageRecord readMessage(ResultSet resultSet) throws SQLException {
        final EntityStorageRecord record = super.readMessage(resultSet);
        if (record == null) {
            return null;
        }
        final boolean archived = resultSet.getBoolean(EntityStatusField.archived.toString());
        final boolean deleted = resultSet.getBoolean(EntityStatusField.deleted.toString());
        if (!(archived || deleted)) {
            return record;
        }
        final EntityStatus status = EntityStatus.newBuilder()
                                                .setArchived(archived)
                                                .setDeleted(deleted)
                                                .build();
        final EntityStorageRecord result = record.toBuilder()
                                                 .setEntityStatus(status)
                                                 .build();
        return result;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends SelectByIdQuery.Builder<Builder<I>,
                                                                   SelectEntityByIdQuery<I>,
                                                                   I,
                                                                   EntityStorageRecord> {

        @Override
        public SelectEntityByIdQuery<I> build() {
            return new SelectEntityByIdQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
