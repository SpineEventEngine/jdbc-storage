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

package org.spine3.server.storage.jdbc.entity.query;

import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.StorageQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link StorageQuery} for bulk selection.
 * <p> Allows to read either all or specific records from a JDBC source.
 *
 * <p>{@code SQL} analogs of this are:
 * <ul>
 *     <li>1. {@code SELECT * FROM table;}
 *     <li>2. {@code SELECT * FROM table WHERE id IN (?,...,?);}
 * <ul/>
 *
 * @author Dmytro Dashenkov
 */
public class SelectBulkQuery extends StorageQuery {

    private final TypeUrl typeUrl;
    private final FieldMask fieldMask;
    private final List arguments;

    private static final String COMMON_TEMPLATE = "SELECT * FROM %s";
    private static final String ALL_TEMPLATE = COMMON_TEMPLATE + ';';
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String IDS_TEMPLATE = COMMON_TEMPLATE + " WHERE " + EntityTable.ID_COL + " IN (%s);";

    private static final int IDS_STRING_ESTIMATED_LENGTH = 128;

    protected SelectBulkQuery(Builder builder) {
        super(builder);
        final Descriptors.Descriptor messageDescriptor = checkNotNull(builder.messageDescriptor);
        this.typeUrl = TypeUrl.of(messageDescriptor);
        this.fieldMask = builder.fieldMask;
        this.arguments = builder.arguments;
    }

    /**
     * Executes the query.
     *
     * @return ID-to-{@link EntityStorageRecord} {@link Map} as the result of the query.
     * @throws SQLException if the input data contained SQL errors or the table does not exist.
     */
    public Map<Object, EntityStorageRecord> execute() throws SQLException {
        final ConnectionWrapper connection = getConnection(true);
        final PreparedStatement sqlStatement = connection.prepareStatement(getQuery());


        for (int i = 0; i < arguments.size(); i++) {
            sqlStatement.setObject(i + 1, arguments.get(i));
        }

        final ResultSet resultSet = sqlStatement.executeQuery();

        connection.close();

        return QueryResults.parse(resultSet, fieldMask, typeUrl);
    }

    /**
     * @return New instance of the {@link Builder} set to "query all" by default.
     */
    public static Builder newBuilder(String tableName) {
        return newBuilder()
                .setAllQuery(tableName);
    }

    /**
     * @return new instance of the {@link Builder}.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * <p>Builds instances of {@code SelectBulkQuery}.
     * <p>All fields are required.
     *
     * <p>One of methods {@link #setAllQuery(String)} and {@link #setIdsQuery(String, Iterable)} should be called
     * before {@link #build()}.
     */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends StorageQuery.Builder<Builder, SelectBulkQuery> {

        private Descriptors.Descriptor messageDescriptor;
        private FieldMask fieldMask;
        private final List<Object> arguments = new ArrayList<>();

        private Builder() {
        }

        public Builder setMessageDescriptor(Descriptors.Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }

        public Builder setFieldMask(FieldMask fieldMask) {
            this.fieldMask = fieldMask;
            return getThis();
        }

        /**
         * Sets {@code SelectBulkQuery} into "query all" mode and sets the table name.
         * Either this method or {@link Builder#setIdsQuery(String, Iterable)} should be called before
         * the {@code SelectBulkQuery} is built.
         *
         * @param tableName Name of the table to query.
         */
        public Builder setAllQuery(String tableName) {
            setQuery(String.format(ALL_TEMPLATE, tableName));
            return getThis();
        }

        /**
         * Sets {@code SelectBulkQuery} into "query by ids" mode and sets the table name.
         * Either this method or {@link Builder#setAllQuery(String)} should be called before
         * the {@code SelectBulkQuery} is built.
         *
         * @param tableName Name of the table to query.
         * @param ids IDs to search for.
         */
        public Builder setIdsQuery(String tableName, Iterable<?> ids) {
            final StringBuilder paramsBuilder = new StringBuilder(IDS_STRING_ESTIMATED_LENGTH);

            final Iterator<?> params = ids.iterator();

            while (params.hasNext()) {
                paramsBuilder.append('?');

                arguments.add(params.next());

                if (params.hasNext()) {
                    paramsBuilder.append(',');
                }
            }

            setQuery(String.format(IDS_TEMPLATE, tableName, paramsBuilder.toString()));
            return getThis();
        }

        /**
         * @return new instance of {@code SelectBulkQuery}.
         */
        @Override
        public SelectBulkQuery build() {
            return new SelectBulkQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
