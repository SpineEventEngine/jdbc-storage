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

import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.TableColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.Query.ALL_ATTRIBUTES;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static java.lang.String.format;

/**
 * A query that checks if the table contains a record with given ID.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public class ContainsQuery<I> extends StorageQuery {

    private static final String FORMAT_PLACEHOLDER = "%s";
    private static final String SQL_TEMPLATE = SELECT.toString() + ALL_ATTRIBUTES +
                                               FROM + FORMAT_PLACEHOLDER +
                                               WHERE + FORMAT_PLACEHOLDER + EQUAL + PLACEHOLDER;

    private final IdColumn<I, ?> idColumn;
    private final I id;

    private ContainsQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    /**
     * @return {@code true} if there is at least one record with given ID, {@code} false otherwise
     */
    public boolean execute() {
        try (ConnectionWrapper connection = getConnection(false);
             PreparedStatement statement = prepareStatementWithParameters(connection)) {
            final ResultSet results = statement.executeQuery();
            final boolean result = results.next();
            results.close();
            return result;
        } catch (SQLException e) {
            getLogger().error("Exception executing statement: " + getQuery(), e);
            throw new DatabaseException(e);
        }
    }

    @Override
    protected IdentifiedParameters getQueryParameters() {
        return IdentifiedParameters.newBuilder()
                                   .addParameter(1, idColumn.normalize(id))
                                   .build();
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageQuery.Builder<Builder<I>, ContainsQuery<I>> {

        private String tableName;
        private IdColumn<I, ?> idColumn;
        private I id;
        private TableColumn keyColumn;

        public Builder<I> setTableName(String tableName) {
            this.tableName = checkNotNull(tableName);
            return this;
        }

        public Builder<I> setIdColumn(IdColumn<I, ?> idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        public Builder<I> setKeyColumn(TableColumn keyColumn) {
            this.keyColumn = keyColumn;
            return this;
        }

        public Builder<I> setId(I id) {
            this.id = id;
            return this;
        }

        @Override
        public ContainsQuery<I> build() {
            final String sql = format(SQL_TEMPLATE, tableName, keyColumn.name());
            setQuery(sql);
            return new ContainsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
