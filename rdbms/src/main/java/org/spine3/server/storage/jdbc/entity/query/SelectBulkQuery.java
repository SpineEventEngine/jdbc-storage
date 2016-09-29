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
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.storage.jdbc.query.Query;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dmytro Dashenkov
 */
public class SelectBulkQuery<M extends Message> extends Query {

    private final Descriptors.Descriptor messageDescriptor;
    private final TypeUrl typeUrl;
    private final FieldMask fieldMask;
    private final List arguments;

    private static final String COMMON_TEMPLATE = "SELECT * FROM %s";
    private static final String ALL_TEMPLATE = COMMON_TEMPLATE + ';';
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String IDS_TEMPLATE = COMMON_TEMPLATE + " WHERE " + EntityTable.ID_COL + " IN (%s);";

    private static final int IDS_STRING_ESTIMATED_LENGTH = 128;

    protected SelectBulkQuery(Builder<M> builder) {
        super(builder);
        this.messageDescriptor = checkNotNull(builder.messageDescriptor);
        this.fieldMask = builder.fieldMask;
        this.typeUrl = TypeUrl.of(messageDescriptor);
        this.arguments = builder.arguments;
    }

    public Map<Object, M> execute() throws SQLException {
        final ConnectionWrapper connection = getConnection(true);
        final PreparedStatement sqlStatement = connection.prepareStatement(getQuery());


        for (int i = 0; i < arguments.size(); i++) {
            sqlStatement.setObject(i + 1, arguments.get(i));
        }

        final ResultSet resultSet = sqlStatement.executeQuery();

        connection.close();

        return QueryResults.parse(resultSet, messageDescriptor, fieldMask, typeUrl);
    }

    public static Builder<?> newBuilder(String tableName) {
        return newBuilder()
                .setAllQuery(tableName);
    }

    public static Builder<?> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<M extends Message> extends Query.Builder<Builder<M>, SelectBulkQuery> {

        private Descriptors.Descriptor messageDescriptor;
        private FieldMask fieldMask;
        private final List<Object> arguments = new ArrayList<>();

        private Builder() {
        }

        public Builder<M> setMessageDescriptor(Descriptors.Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }

        public Builder<M> setFieldMask(FieldMask fieldMask) {
            this.fieldMask = fieldMask;
            return getThis();
        }

        public Builder<M> setAllQuery(String tableName) {
            setQuery(String.format(ALL_TEMPLATE, tableName));
            return getThis();
        }

        public Builder<M> setIdsQuery(String tableName, Iterable<?> ids) {
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

        @Override
        public Builder<M> setQuery(String query) {
            return super.setQuery(query);
        }

        @Override
        public Builder<M> setDataSource(DataSourceWrapper dataSource) {
            return super.setDataSource(dataSource);
        }

        @Override
        public Builder<M> setLogger(Logger logger) {
            return super.setLogger(logger);
        }

        @Override
        public SelectBulkQuery<M> build() {
            return new SelectBulkQuery<>(this);
        }

        @Override
        protected Builder<M> getThis() {
            return this;
        }
    }
}
