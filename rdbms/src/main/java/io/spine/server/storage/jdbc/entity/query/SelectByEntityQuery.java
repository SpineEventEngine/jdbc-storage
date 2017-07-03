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

package io.spine.server.storage.jdbc.entity.query;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.FieldMask;
import io.spine.client.ColumnFilter;
import io.spine.protobuf.TypeConverter;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.query.StorageQuery;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.valueOf;
import static io.spine.server.storage.jdbc.Sql.Query.ALL_ATTRIBUTES;
import static io.spine.server.storage.jdbc.Sql.Query.AND;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.IN;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static io.spine.server.storage.jdbc.table.entity.RecordTable.StandardColumn.id;
import static java.lang.String.format;

/**
 * Implementation of {@link StorageQuery} for bulk selection.
 * <p> Allows to read either all or specific records from a JDBC source.
 *
 * <p>{@code SQL} analogs of this are:
 * <ul>
 * <li>1. {@code SELECT * FROM table;}
 * <li>2. {@code SELECT * FROM table WHERE id IN (?,...,?);}
 * <ul/>
 *
 * @author Dmytro Dashenkov
 */
public class SelectByEntityQuery<I> extends StorageQuery {

    private static String commonTemplate =
            SELECT.toString() + ALL_ATTRIBUTES +
            FROM + " %s " +
            WHERE + " %s " + id + IN
            + " %s" + SEMICOLON;

    private final FieldMask fieldMask;
    private final List<I> arguments;
    private final IdColumn<I> idColumn;
    private final Map<Column, ColumnFilter> columns;

    protected SelectByEntityQuery(Builder<I> builder) {
        super(builder);
        this.fieldMask = builder.fieldMask;
        this.arguments = builder.arguments;
        this.idColumn = builder.idColumn;
        this.columns = builder.columns;
    }

    /**
     * Executes the query.
     *
     * @return ID-to-{@link EntityRecord} {@link Map} as the result of the query.
     * @throws SQLException if the input data contained SQL errors or the table does not exist.
     */
    public Map<I, EntityRecord> execute() throws SQLException {
        final ConnectionWrapper connection = getConnection(true);
        final PreparedStatement sqlStatement = connection.prepareStatement(getQuery());

        for (int i = 0; i < arguments.size(); i++) {
            idColumn.setId(i + 1, arguments.get(i), sqlStatement);
        }

        prepareStatement(sqlStatement);

        final ResultSet resultSet = sqlStatement.executeQuery();

        connection.close();

        return QueryResults.parse(resultSet, fieldMask);
    }

    private void prepareStatement(PreparedStatement sqlStatement) {
        final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry =
                JdbcTypeRegistryFactory.defaultInstance();
        JdbcColumnType columnType;
        int columnIdentifier = columns.size();
        Object columnValue;

        for (Map.Entry<Column, ColumnFilter> column : columns.entrySet()) {
            columnValue = TypeConverter.toObject(column.getValue()
                                                       .getValue(), column.getKey()
                                                                          .getType());
            columnType = columnTypeRegistry.get(column.getKey());
            setValue(columnValue, sqlStatement, columnIdentifier, columnType);
            columnIdentifier--;
        }
    }

    @SuppressWarnings("unchecked") // Checked at runtime
    private static void setValue(Object columnValue,
                                 PreparedStatement statement,
                                 int columnIdentifier,
                                 JdbcColumnType columnType) {

        final Object value = columnType.convertColumnValue(columnValue);
        columnType.setColumnValue(statement, value, columnIdentifier);
    }

    /**
     * @return new instance of the {@link Builder}.
     */
    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * <p>Builds instances of {@code SelectBulkQuery}.
     * <p>All fields are required.
     */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder<I> extends StorageQuery.Builder<Builder<I>, SelectByEntityQuery> {

        private FieldMask fieldMask;
        private final List<I> arguments = new ArrayList<>();
        private IdColumn<I> idColumn;
        private final Map<Column, ColumnFilter> columns = new HashMap<>();

        private Builder() {
            super();
        }

        public Builder<I> setFieldMask(FieldMask fieldMask) {
            this.fieldMask = fieldMask;
            return getThis();
        }


        @SuppressWarnings("MethodWithMultipleLoops") //It's ok since we need to go
                                                     // through nested collections
        public Builder<I> setQueryByEntity(EntityQuery<I> query, String tableName) {
            final QueryParameters compositeParameters = query.getParameters();
            StringBuilder parameters = new StringBuilder();

            for (CompositeQueryParameter parameter : compositeParameters) {
                final ImmutableMultimap<Column, ColumnFilter> filters = parameter.getFilters();

                parameters.append(BRACKET_OPEN);

                for (int i = 0; i < filters.size(); i++) {
                    final Column column = filters.keySet()
                                                 .asList()
                                                 .get(i);
                    final ColumnFilter value = filters.values()
                                                      .asList()
                                                      .get(i);
                    columns.put(column, value);
                    final boolean isLastFilter = i == filters.size() - 1;
                    final Sql.BuildingBlock operator = valueOf(value.getOperator()
                                                                    .toString());

                    if (isLastFilter) {
                        parameters.append(value.getColumnName())
                                  .append(operator)
                                  .append(PLACEHOLDER).append(' ');
                    } else {
                        parameters.append(value.getColumnName())
                                  .append(operator)
                                  .append(PLACEHOLDER).append(' ')
                                  .append(Sql.Query.valueOf(parameter.getOperator().toString()))
                                  .append(' ');
                    }
                }

                parameters.append(BRACKET_CLOSE)
                          .append(AND);
            }

            final ImmutableSet<I> queryIds = query.getIds();
            final int idsCount = queryIds.size();

            final String placeholders;
            if (idsCount == 0) {
                parameters.delete(parameters.length() - 4, parameters.length());
                commonTemplate = commonTemplate.substring(0, commonTemplate.length() - 12) +
                                 SEMICOLON;
                placeholders = "";
            } else {
                placeholders = Sql.nPlaceholders(idsCount);
            }

            addArguments(queryIds);

            final String querySql = format(commonTemplate, tableName, parameters, placeholders);

            setQuery(querySql);
            return getThis();
        }

        private void addArguments(ImmutableSet<I> queryIds) {
            for (I id : queryIds) {
                arguments.add(id);
            }
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        /**
         * @return new instance of {@code SelectBulkQuery}.
         */
        @Override
        public SelectByEntityQuery<I> build() {
            return new SelectByEntityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
