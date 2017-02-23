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

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.ALL_ATTRIBUTES;
import static org.spine3.server.storage.jdbc.Sql.Query.AND;
import static org.spine3.server.storage.jdbc.Sql.Query.FALSE;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.IN;
import static org.spine3.server.storage.jdbc.Sql.Query.IS;
import static org.spine3.server.storage.jdbc.Sql.Query.NULL;
import static org.spine3.server.storage.jdbc.Sql.Query.OR;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;

/**
 * Implementation of {@link StorageQuery} for bulk selection.
 * <p> Allows to read either all or specific records from a JDBC source.
 *
 * <p>{@code SQL} analogs of this are:
 * <ul>
 * <li>1. {@code SELECT * FROM table;}
 * <li>2. {@code SELECT * FROM table WHERE id IN (?,...,?);}
 * <ul/>
 *
 * @author Dmytro Dashenkov
 */
public class SelectBulkQuery<I> extends StorageQuery {

    private final TypeUrl typeUrl;
    private final FieldMask fieldMask;
    private final List<I> arguments;
    private final IdColumn<I> idColumn;

    private static final String COMMON_TEMPLATE =
            SELECT.toString() + ALL_ATTRIBUTES +
            FROM + "%s" +
            WHERE + BRACKET_OPEN + archived + IS + NULL + OR +
            archived + EQUAL + FALSE + BRACKET_CLOSE + AND +
            BRACKET_OPEN + deleted + IS + NULL + OR +
            deleted + EQUAL + FALSE + BRACKET_CLOSE;

    private static final String ALL_TEMPLATE = COMMON_TEMPLATE + ';';
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String IDS_TEMPLATE = COMMON_TEMPLATE + AND + EntityTable.ID_COL + IN
                                               + " %s" + SEMICOLON;

    protected SelectBulkQuery(Builder<I> builder) {
        super(builder);
        final Descriptors.Descriptor messageDescriptor = checkNotNull(builder.messageDescriptor);
        this.typeUrl = TypeUrl.from(messageDescriptor);
        this.fieldMask = builder.fieldMask;
        this.arguments = builder.arguments;
        this.idColumn = builder.idColumn;
    }

    /**
     * Executes the query.
     *
     * @return ID-to-{@link EntityRecord} {@link Map} as the result of the query.
     * @throws SQLException if the input data contained SQL errors or the table does not exist.
     */
    public Map<Object, EntityRecord> execute() throws SQLException {
        final ConnectionWrapper connection = getConnection(true);
        final PreparedStatement sqlStatement = connection.prepareStatement(getQuery());

        for (int i = 0; i < arguments.size(); i++) {
            idColumn.setId(i + 1, arguments.get(i), sqlStatement);
            //sqlStatement.setObject(i + 1, arguments.get(i));
        }

        final ResultSet resultSet = sqlStatement.executeQuery();

        connection.close();

        return QueryResults.parse(resultSet, fieldMask, typeUrl);
    }

    /**
     * @return New instance of the {@link Builder} set to "query all" by default.
     */
    public static <I> Builder<I> newBuilder(String tableName) {
        return SelectBulkQuery.<I>newBuilder()
                .setAllQuery(tableName);
    }

    /**
     * @return new instance of the {@link Builder}.
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * <p>Builds instances of {@code SelectBulkQuery}.
     * <p>All fields are required.
     *
     * <p>One of methods {@link #setAllQuery(String)} and {@link #setIdsQuery(String, Iterable)} should be called
     * before {@link #build()}.
     */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends StorageQuery.Builder<Builder<I>, SelectBulkQuery> {

        private Descriptors.Descriptor messageDescriptor;
        private FieldMask fieldMask;
        private final List<I> arguments = new ArrayList<>();
        private IdColumn<I> idColumn;

        private Builder() {
            super();
        }

        public Builder<I> setMessageDescriptor(Descriptors.Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }

        public Builder<I> setFieldMask(FieldMask fieldMask) {
            this.fieldMask = fieldMask;
            return getThis();
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        /**
         * Sets {@code SelectBulkQuery} into "query all" mode and sets the table name.
         * Either this method or {@link Builder#setIdsQuery(String, Iterable)} should be called before
         * the {@code SelectBulkQuery} is built.
         *
         * @param tableName Name of the table to query.
         */
        public Builder<I> setAllQuery(String tableName) {
            setQuery(String.format(ALL_TEMPLATE, tableName));
            return getThis();
        }

        /**
         * Sets {@code SelectBulkQuery} into "query by ids" mode and sets the table name.
         * Either this method or {@link Builder#setAllQuery(String)} should be called before
         * the {@code SelectBulkQuery} is built.
         *
         * @param tableName Name of the table to query.
         * @param ids       IDs to search for.
         */
        public Builder<I> setIdsQuery(String tableName, Iterable<I> ids) {
            final Collection<I> idsCollection = Lists.newArrayList(ids);
            final int idsCount = idsCollection.size();

            final String placeholders;
            if (idsCount == 0) {
                placeholders = BRACKET_OPEN.toString() + BRACKET_CLOSE;
            } else {
                placeholders = Sql.nPlaceholders(idsCount);
            }
            for (I id : ids) {
                arguments.add(id);
            }

            setQuery(String.format(IDS_TEMPLATE, tableName, placeholders));
            return getThis();
        }

        /**
         * @return new instance of {@code SelectBulkQuery}.
         */
        @Override
        public SelectBulkQuery build() {
            return new SelectBulkQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
