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

package io.spine.server.storage.jdbc.entity.query;

import io.spine.server.entity.EntityRecord;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;
import io.spine.server.storage.jdbc.table.entity.RecordTable;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.ALL_ATTRIBUTES;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static io.spine.server.storage.jdbc.table.entity.RecordTable.StandardColumn.entity;
import static java.lang.String.format;

/**
 * Query that selects {@link EntityRecord} by ID.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectEntityByIdQuery<I> extends SelectMessageByIdQuery<I, EntityRecord> {

    private static final String QUERY_TEMPLATE =
            SELECT.toString() + ALL_ATTRIBUTES + FROM + " %s" + WHERE +
            RecordTable.StandardColumn.id + EQUAL + PLACEHOLDER + SEMICOLON;

    public SelectEntityByIdQuery(Builder<I> builder) {
        super(builder);
    }

    public static <I> Builder<I> newBuilder(String tableName) {
        final Builder<I> builder = new Builder<>();
        builder.setIdIndexInQuery(1)
               .setQuery(format(QUERY_TEMPLATE, tableName))
               .setMessageColumnName(entity.name())
               .setMessageDescriptor(EntityRecord.getDescriptor());
        return builder;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overridden to expose this method to {@code io.spine.server.storage.jdbc.entity.query}
     * package.
     */
    @Nullable
    @Override
    protected EntityRecord readMessage(ResultSet resultSet) throws SQLException {
        return super.readMessage(resultSet);
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends SelectMessageByIdQuery.Builder<Builder<I>,
                                                                          SelectEntityByIdQuery<I>,
                                                                          I,
                                                                          EntityRecord> {

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
