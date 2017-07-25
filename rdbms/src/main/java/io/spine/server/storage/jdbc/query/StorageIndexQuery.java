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

package io.spine.server.storage.jdbc.query;

import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.IndexIterator;

import java.sql.PreparedStatement;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static java.lang.String.format;

/**
 * A query for all the IDs in a certain table.
 *
 * @author Dmytro Dashenkov
 */
public class StorageIndexQuery<I> extends StorageQuery {

    private static final String FORMAT = "%s";

    private static final String SQL_TEMPLATE = SELECT + FORMAT + FROM + FORMAT + SEMICOLON;

    private final String idColumnName;
    private final Class<I> idType;

    private StorageIndexQuery(Builder<I> builder) {
        super(builder);
        this.idColumnName = builder.getIdColumnName();
        this.idType = builder.getIdType();
    }

    public Iterator<I> execute() {
        try (ConnectionWrapper connection = getConnection(true)) {
            final PreparedStatement statement = prepareStatement(connection);
            final Iterator<I> result = IndexIterator.create(statement, idColumnName, idType);
            return result;
        }
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageQuery.Builder<Builder<I>, StorageIndexQuery<I>> {

        private static final String DEFAULT_ID_COLUMN_NAME = "id";

        private String idColumnName = DEFAULT_ID_COLUMN_NAME;
        private String tableName;
        private Class<I> idType;

        private Builder() {
            super();
            // Prevent direct instantiation.
        }

        public String getIdColumnName() {
            return idColumnName;
        }

        public Builder<I> setIdColumnName(String idColumnName) {
            this.idColumnName = checkNotNull(idColumnName);
            return getThis();
        }

        public Builder<I> setTableName(String tableName) {
            this.tableName = tableName;
            return getThis();
        }

        private Class<I> getIdType() {
            return idType;
        }

        public Builder<I> setIdType(Class<I> idType) {
            this.idType = idType;
            return getThis();
        }

        @Override
        public StorageIndexQuery<I> build() {
            checkState(tableName != null, "Table name is not set.");
            checkState(idType!= null, "ID type is not set.");
            setQuery(format(SQL_TEMPLATE, idColumnName, tableName));
            return new StorageIndexQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
