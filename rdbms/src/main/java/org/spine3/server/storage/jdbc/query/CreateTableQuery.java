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

import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;

/**
 * A query that creates data table if it does not exist.
 *
 * @author Andrey Lavrov
 */
@Internal
public class CreateTableQuery<I> extends Query {

    @Nullable
    private final IdColumn<I> idColumn;
    private final String tableName;

    protected CreateTableQuery(Builder<? extends Builder, ? extends CreateTableQuery, I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.tableName = builder.tableName;
    }

    /**
     * Executes a create table query.
     * Creates tables with ID column if idColumn is null, or with out it otherwise.
     */
    public void execute() {
        final String sql;
        if (idColumn != null) {
            final String idColumnType = idColumn.getColumnDataType();
            sql = format(getQuery(), tableName, idColumnType);
        } else {
            sql = format(getQuery(), tableName);
        }
        try (ConnectionWrapper connection = this.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            this.getLogger().error("Error while creating a table with the name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public abstract static class Builder<B extends Builder<B, Q, I>, Q extends CreateTableQuery, I>
            extends Query.Builder<B, Q> {

        private IdColumn<I> idColumn;
        private String tableName;

        public B setIdColumn(IdColumn<I> idColumn){
            this.idColumn = idColumn;
            return getThis();
        }

        public B setTableName(String tableName){
            this.tableName = tableName;
            return getThis();
        }
    }
}
