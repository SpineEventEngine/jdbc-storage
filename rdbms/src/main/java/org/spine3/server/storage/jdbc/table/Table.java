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

package org.spine3.server.storage.jdbc.table;

import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.util.DbTableNameFactory.newTableName;

/**
 * @author Dmytro Dashenkov.
 */
public class Table<I> {

    private final String name;

    private final List<Column> columns;

    private final IdColumn<I> idColumn;

    private Table(Builder<I> builder) {
        this.name = builder.name;
        this.columns = builder.columns;
        this.idColumn = builder.idColumn;
    }

    public Column getColumn(int index) throws IndexOutOfBoundsException {
        checkElementIndex(index,
                          columns.size(),
                          "Column index unavailable: " + String.valueOf(index));
        final Column result = columns.get(index);
        return result;
    }

    public int getColumnIndex(String columnName) throws IllegalArgumentException {
        checkArgument(!isNullOrEmpty(columnName), "Column name must not be null or empty");

        for (int i = 0; i < columns.size(); i++) {
            final Column column = columns.get(i);
            final String name = column.getName();
            if (name.equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(format("Column %s is not present in table %s.",
                                                  columnName,
                                                  this.name));
    }

    public int getColumnCount() {
        return columns.size();
    }

    public void addAgruments(PreparedStatement sqlStatement, Map<Column, Object> argumentValues) {
        checkNotNull(sqlStatement, "sqlStatement");
        checkNotNull(argumentValues, "argumentValues");
        checkArgument(!argumentValues.isEmpty(), "Argument values Map must not be empty.");


    }

    public static class Column {

        private final String name;

        private final Sql.Type type;

        private Column(String name, Sql.Type type) {
            this.name = checkNotNull(name);
            this.type = checkNotNull(type);
        }

        public String getName() {
            return name;
        }

        public Sql.Type getType() {
            return type;
        }
    }

    public static class Builder<I> {

        private static final int DEFAULT_COLUMNS_COUNT = 5;

        private final List<Column> columns = new ArrayList<>(DEFAULT_COLUMNS_COUNT);
        private String name;
        private IdColumn<I> idColumn;

        public Builder<I> setName(String name) {
            checkArgument(!isNullOrEmpty(name), "Table name must not be null or empty.");
            this.name = name;
            return this;
        }

        public Builder<I> setName(Class<? extends Entity<I, ?>> entityClass) {
            checkNotNull(entityClass);
            this.name = newTableName(entityClass);
            return this;
        }

        public Builder<I> addColumn(String name, Sql.Type type) {
            final Column column = new Column(
                    checkNotNull(name),
                    checkNotNull(type));
            columns.add(column);
            return this;
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return this;
        }

        public Table<I> createTable() {
            checkPreconditions();
            return new Table<>(this);
        }

        private void checkPreconditions() {
            checkNotNull(idColumn, "ID column  must be set.");
            checkNotNull(name, "Table name must be set.");
            checkState(!columns.isEmpty(), "At least one column must be set.");
        }
    }
}
