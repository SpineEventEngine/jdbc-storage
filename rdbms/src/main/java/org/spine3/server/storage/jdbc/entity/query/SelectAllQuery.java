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
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.SqlExecutionHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dmytro Dashenkov
 */
public class SelectAllQuery<M extends Message> extends Query {

    private final Descriptors.Descriptor messageDescriptor;
    private final TypeUrl typeUrl;
    private final FieldMask fieldMask;

    private static final String COMMON_TEMPLATE = "SELECT * FROM TABLE %s";
    private static final String ALL_TEMPLATE = COMMON_TEMPLATE + ';';
    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String IDS_TEMPLATE = COMMON_TEMPLATE + " WHERE " + EntityTable.ID_COL + " IN (%s);";

    private static final int IDS_STRING_ESTIMATED_LENGTH = 128;

    protected SelectAllQuery(Builder<M> builder) {
        super(builder);
        this.messageDescriptor = checkNotNull(builder.messageDescriptor);
        this.fieldMask = builder.fieldMask;
        this.typeUrl = TypeUrl.of(messageDescriptor);
    }

    public Map<Object, M> execute() throws SQLException {
        final ResultSet resultSet = SqlExecutionHelper.execute(getQuery(), getConnection(true));

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
    public static class Builder<M extends Message> extends Query.Builder<Builder<M>, SelectAllQuery> {

        private Descriptors.Descriptor messageDescriptor;
        private FieldMask fieldMask;

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
                final String oneId = String.valueOf(params.next());

                paramsBuilder.append(oneId);

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
        public SelectAllQuery<M> build() {
            return new SelectAllQuery<>(this);
        }

        @Override
        protected Builder<M> getThis() {
            return this;
        }
    }
}
