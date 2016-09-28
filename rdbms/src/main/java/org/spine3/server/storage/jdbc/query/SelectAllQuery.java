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

package org.spine3.server.storage.jdbc.query;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.entity.FieldMasks;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.Serializer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dmytro Dashenkov
 */
public class SelectAllQuery<M extends Message> extends Query {

    private final Descriptors.Descriptor messageDescriptor;
    private final TypeUrl typeUrl;
    private final String messageColumnLabel;
    private final FieldMask fieldMask;

    private static final String TEMPLATE = "SELECT * FROM TABLE %s;";

    protected SelectAllQuery(Builder<M> builder) {
        super(builder);
        this.messageDescriptor = checkNotNull(builder.messageDescriptor);
        this.messageColumnLabel = checkNotNull(builder.messageColumnLabel);
        this.fieldMask = builder.fieldMask;
        this.typeUrl = TypeUrl.of(messageDescriptor);
    }

    public Collection<M> execute() throws SQLException {
        final String sql = getQuery();

        final PreparedStatement sqlStatement;

        try (ConnectionWrapper connection = getConnection(true)) {
            sqlStatement = connection.prepareStatement(sql);
        }

        final ResultSet resultSet = sqlStatement.executeQuery();

        final ImmutableList.Builder<M> resultListBuilder = new ImmutableList.Builder<>();

        while (resultSet.next()) {
            final M message = readSingleMessage(resultSet);
            final M maskedMessage = maskFields(message);
            resultListBuilder.add(maskedMessage);
        }

        resultSet.close();

        return resultListBuilder.build();
    }

    private M readSingleMessage(ResultSet resultSet) throws SQLException {
        return Serializer.deserialize(resultSet.getBytes(messageColumnLabel), messageDescriptor);
    }

    private M maskFields(M message) {
        if (fieldMask != null) {
            return FieldMasks.applyMask(fieldMask, message, typeUrl);
        }

        return message;
    }

    public static Builder newBuilder(String tableName) {
        return new Builder<>()
                .setQuery(String.format(TEMPLATE, tableName));
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<M extends Message> extends Query.Builder<Builder<M>, SelectAllQuery> {

        private Descriptors.Descriptor messageDescriptor;
        private String messageColumnLabel;
        private FieldMask fieldMask;

        private Builder() {
        }

        public Builder<M> setMessageDescriptor(Descriptors.Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }

        public Builder<M> setMessageColumnLabel(String messageColumnLabel) {
            this.messageColumnLabel = messageColumnLabel;
            return getThis();
        }

        public Builder<M> setFieldMask(FieldMask fieldMask) {
            this.fieldMask = fieldMask;
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
