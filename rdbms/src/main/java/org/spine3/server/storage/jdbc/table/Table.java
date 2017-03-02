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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.util.IdColumn;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.CREATE_IF_MISSING;
import static org.spine3.server.storage.jdbc.Sql.Query.PRIMARY_KEY;
import static org.spine3.server.storage.jdbc.util.DbTableNameFactory.newTableName;

/**
 * @author Dmytro Dashenkov.
 */
public class Table<I> {

    private static final int DEFAULT_SQL_QUERY_LENGTH = 128;

    private final String name;

    private final List<Column> columns;

    private final String idColumnName;

    private final IdColumn<I> idColumn;

    private Table(Builder<I> builder) {
        this.name = builder.name;
        this.columns = builder.columns;
        this.idColumnName = builder.idColumnName;
        this.idColumn = builder.idColumn;
    }

    public Column getColumn(int index) throws IndexOutOfBoundsException {
        checkElementIndex(index,
                          columns.size(),
                          "Column index unavailable: " + String.valueOf(index));
        final Column result = columns.get(index);
        return result;
    }

    public IdColumn<I> getIdColumn() {
        return idColumn;
    }

    public String getName() {
        return name;
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

    public String columnNames() {
        final Function<Column, String> nameUnPacker = new Function<Column, String>() {
            @Override
            public String apply(@Nullable Column column) {
                checkNotNull(column);
                return column.getName();
            }
        };

        final Collection<String> names = Collections2.transform(columns, nameUnPacker);
        final String joinedNames = Joiner.on(COMMA.toString())
                                         .join(names);
        return joinedNames;
    }

    public String createTableSql() {
        @SuppressWarnings("StringBufferReplaceableByString")
        final StringBuilder sql = new StringBuilder(DEFAULT_SQL_QUERY_LENGTH);
        sql.append(CREATE_IF_MISSING)
           .append(getName())
           .append(BRACKET_OPEN);
        //  .append(columnNames())
        for (Column column : columns) {
            sql.append(column.getName())
               .append(column.getType())
               .append(COMMA);
            // Comma after the last column declaration is required since we add PRIMARY KEY after
        }
        sql.append(PRIMARY_KEY)
           .append(idColumnName)
           .append(BRACKET_CLOSE)
           .append(SEMICOLON);
        final String result = sql.toString();
        return result;
    }

    public void fillDerectOrderParams(PreparedStatement sqlStatement, Object... queryParams)
            throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            final Column column = columns.get(i);
            final Object parameter = queryParams[i];
            setParameter(sqlStatement, column, parameter, i);
        }
    }

    public void fillParamsWithIdAtTheEnd(PreparedStatement sqlStatement, Object... queryParams)
            throws SQLException {
        int position = 1;
        for (final Column column : columns) {
            if (column.getName()
                      .equals(idColumnName)) {
                continue;
            }
            final Object parameter = queryParams[position];
            setParameter(sqlStatement, column, parameter, position);
            ++position;
        }

        @SuppressWarnings("unchecked")
        final I id = (I) queryParams[queryParams.length - 1];
        idColumn.setId(position, id, sqlStatement);
    }

    private static void setParameter(PreparedStatement sqlStatement,
                                     Column column,
                                     Object value,
                                     int position) throws SQLException {
        final Sql.Type type = column.getType();
        switch (type) {
            case BLOB:
                final byte[] bytes = (byte[]) value;
                sqlStatement.setBytes(position, bytes);
                break;
            case INT:
                final int number = (int) value;
                sqlStatement.setInt(position, number);
                break;
            case BIGINT:
                final long longNumber = (long) value;
                sqlStatement.setLong(position, longNumber);
                break;
            case VARCHAR_512: // All VARCHAR types are Java Strings
            case VARCHAR_999:
                final String stringValue = (String) value;
                sqlStatement.setString(position, stringValue);
                break;
            case BOOLEAN:
                final boolean logicalValue = (boolean) value;
                sqlStatement.setBoolean(position, logicalValue);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unhandled SQL type \"" + type.toString() + '\"');
        }
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

        private static final String DEFAULT_ID_COLUMN_NAME = "id";

        private final List<Column> columns = new ArrayList<>(DEFAULT_COLUMNS_COUNT);
        private String name;
        private IdColumn<I> idColumn;
        private String idColumnName = DEFAULT_ID_COLUMN_NAME;

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
            return setIdColumn(DEFAULT_ID_COLUMN_NAME, idColumn);
        }

        public Builder<I> setIdColumn(String idColumnName, IdColumn<I> idColumn) {
            checkArgument(!isNullOrEmpty(idColumnName), "ID column cannot be null or empty");
            this.idColumn = checkNotNull(idColumn);
            this.idColumnName = idColumnName;
            final Column column = new Column(idColumnName,
                                             Sql.Type.valueOf(idColumn.getColumnDataType()));
            columns.add(column);
            return this;
        }

        public Table<I> build() {
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
