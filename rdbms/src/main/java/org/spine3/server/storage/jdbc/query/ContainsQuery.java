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

package org.spine3.server.storage.jdbc.query;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.table.TableColumn;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.Query.ALL_ATTRIBUTES;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;

/**
 * @author Dmytro Dashenkov.
 */
public class ContainsQuery<I> extends StorageQuery {

    private static final String SQL_TEMPLATE = SELECT.toString() + ALL_ATTRIBUTES + FROM + "%s" +
                                               WHERE + "%s" + EQUAL + PLACEHOLDER;

    private final IdColumn<I> idColumn;
    private final I id;

    protected ContainsQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        idColumn.setId(1, id, statement);
        return statement;
    }

    public boolean execute() {
        try (ConnectionWrapper connection = getConnection(false);
             PreparedStatement statement = prepareStatement(connection)) {
            final ResultSet results = statement.executeQuery();
            boolean result = results.next();
            results.close();
            return result;
        } catch (SQLException e) {
            getLogger().error("Exception executing statement: " + getQuery(), e);
            throw new DatabaseException(e);
        }
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageQuery.Builder<Builder<I>, ContainsQuery<I>> {

        private String tableName;
        private IdColumn<I> idColumn;
        private I id;
        private TableColumn keyColumn;

        public Builder<I> setTableName(String tableName) {
            this.tableName = checkNotNull(tableName);
            return this;
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
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
