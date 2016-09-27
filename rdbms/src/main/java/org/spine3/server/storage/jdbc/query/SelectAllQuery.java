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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.Serializer;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Dmytro Dashenkov
 */
public class SelectAllQuery<M extends Message> extends Query {

    private final Descriptors.Descriptor messageDescriptor;
    private final String messageColumnLabel;
    private final Collection<String> queriedFields;

    private static final int ESTIMATED_QUERY_STRING_LIENGTH = 256;

    private static final Function<String, String> fqnToShortName = new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String input) {
            if (input == null) {
                return null;
            }

            final int startIndex = input.lastIndexOf('.');
            if (startIndex < 0) {
                return input;
            }

            return input.substring(startIndex);
        }
    };

    protected SelectAllQuery(Builder builder) {
        super(builder);
        this.messageDescriptor = builder.messageDescriptor;
        this.messageColumnLabel = builder.messageColumnLabel;

        if (builder.fieldMask == null) {
            this.queriedFields = Collections.emptyList();
        } else {
            final Collection<String> fieldPaths = builder.fieldMask.getPathsList();
            this.queriedFields = fieldPaths.isEmpty() ? Collections.<String>emptyList() : Collections2.transform(fieldPaths, fqnToShortName);
        }

    }

    public Collection<M> execute() throws SQLException {
        final String sql = getMaskedQuery();

        final PreparedStatement sqlStatement;

        try (ConnectionWrapper connection = getConnection(true)) {
            sqlStatement = connection.prepareStatement(sql);
        }

        final ResultSet resultSet = sqlStatement.executeQuery();

        final ImmutableList.Builder<M> resultListBuilder = new ImmutableList.Builder<>();

        while (resultSet.next()) {
            final M message = readSingleMessage(resultSet);
            resultListBuilder.add(message);
        }

        resultSet.close();

        return resultListBuilder.build();
    }

    private String getMaskedQuery() {
        final String query = getQuery();

        if (queriedFields.isEmpty()) {
            return query;
        }

        final int fieldsIndex = query.indexOf('*');

        if (fieldsIndex < 0) {
            return query;
        }

        final StringBuilder queryBuilder = new StringBuilder(ESTIMATED_QUERY_STRING_LIENGTH);

        final String queryStart = query.substring(0, fieldsIndex).trim();
        queryBuilder.append(queryStart);
        queryBuilder.append('(');


        // Need to know if there is a next step before loop check
        final Iterator<String> queriedFieldsIterator = queriedFields.iterator();
        while (queriedFieldsIterator.hasNext()) {
            queryBuilder.append(queriedFieldsIterator.next());

            if (queriedFieldsIterator.hasNext()) {
                queryBuilder.append(", ");
            }
        }

        final String queryEnd = query.substring(fieldsIndex + 1).trim();

        queryBuilder.append(')');
        queryBuilder.append(queryEnd);

        return queryBuilder.toString();

    }

    private M readSingleMessage(ResultSet resultSet) throws SQLException {
        return Serializer.deserialize(resultSet.getBytes(messageColumnLabel), messageDescriptor);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends Query.Builder<Builder, SelectAllQuery> {

        private Descriptors.Descriptor messageDescriptor;
        private String messageColumnLabel;
        private FieldMask fieldMask;

        private Builder() {
        }

        public Builder setMessageDescriptor(Descriptors.Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }

        public Builder setMessageColumnLabel(String messageColumnLabel) {
            this.messageColumnLabel = messageColumnLabel;
            return getThis();
        }

        public Builder setFieldMask(FieldMask fieldMask) {
            this.fieldMask = fieldMask;
            return getThis();
        }

        @Override
        public SelectAllQuery build() {
            return new SelectAllQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
