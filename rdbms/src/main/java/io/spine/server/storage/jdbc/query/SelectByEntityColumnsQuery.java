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

import com.google.common.base.Objects;
import com.google.common.collect.UnmodifiableIterator;
import com.google.protobuf.FieldMask;
import io.spine.annotation.Internal;
import io.spine.client.ColumnFilter;
import io.spine.client.ColumnFilter.Operator;
import io.spine.client.CompositeColumnFilter.CompositeOperator;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.RecordTable;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static io.spine.protobuf.TypeConverter.toObject;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.entity;
import static io.spine.server.storage.jdbc.RecordTable.StandardColumn.id;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.GREATER_OR_EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.GREATER_THAN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.LESS_OR_EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.LESS_THAN;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.AND;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.IN;
import static io.spine.server.storage.jdbc.Sql.Query.OR;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static io.spine.server.storage.jdbc.Sql.nPlaceholders;
import static io.spine.util.Exceptions.newIllegalArgumentException;
import static java.util.Collections.emptyMap;

/**
 * A query selecting the records from
 * the {@link RecordTable RecordTable} by
 * an {@link EntityQuery}.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public final class SelectByEntityColumnsQuery<I> extends StorageQuery {

    private static final String COMMON_SQL = SELECT.toString() + entity + FROM + "%s ";

    private final IdentifiedParameters parameters;
    private final FieldMask fieldMask;

    private SelectByEntityColumnsQuery(Builder<I> builder) {
        super(builder);
        this.parameters = builder.parametersBuilder.build();
        this.fieldMask = builder.getFieldMask();
    }

    public Iterator<EntityRecord> execute() {
        final ConnectionWrapper connection = getConnection(true);
        final PreparedStatement statement = prepareStatementWithParameters(connection);
        try {
            final ResultSet resultSet = statement.executeQuery();
            return QueryResults.parse(resultSet, fieldMask);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    protected IdentifiedParameters getQueryParameters() {
        return parameters;
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageQuery.Builder<Builder<I>,
                                                                SelectByEntityColumnsQuery<I>> {

        private EntityQuery<I> entityQuery;
        private FieldMask fieldMask;
        private ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;
        private IdColumn<I, ?> idColumn;
        private String tableName;

        private final IdentifiedParameters.Builder parametersBuilder = IdentifiedParameters.newBuilder();

        private Builder() {
            super();
            fieldMask = FieldMask.getDefaultInstance();
        }

        public Builder<I> setEntityQuery(EntityQuery<I> entityQuery) {
            this.entityQuery = checkNotNull(entityQuery);
            return this;
        }

        private FieldMask getFieldMask() {
            return fieldMask;
        }

        public Builder<I> setFieldMask(FieldMask fieldMask) {
            this.fieldMask = checkNotNull(fieldMask);
            return this;
        }

        public Builder<I> setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> registry) {
            this.columnTypeRegistry = checkNotNull(registry);
            return this;
        }

        public Builder<I> setIdColumn(IdColumn<I, ?> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return this;
        }

        public Builder<I> setTableName(String tableName) {
            this.tableName = checkNotNull(tableName);
            return this;
        }

        private IdColumn<I, ?> getIdColumn() {
            return checkNotNull(idColumn);
        }

        private String getTableName() {
            return checkNotNull(tableName);
        }

        @SuppressWarnings("MethodWithMoreThanThreeNegations") // OK for this method.
        @Override
        public SelectByEntityColumnsQuery<I> build() {
            checkState(entityQuery != null, "EntityQuery is not set.");
            checkState(columnTypeRegistry != null, "ColumnTypeRegistry is not set.");

            final StringBuilder sql = new StringBuilder(String.format(COMMON_SQL, getTableName()));
            final QueryParameters parameters = entityQuery.getParameters();
            final Collection<I> ids = entityQuery.getIds();
            final Iterator<CompositeQueryParameter> iterator = parameters.iterator();
            final Map<ColumnFilterIdentity, Integer> columnIndexes;
            if (iterator.hasNext()) {
                sql.append(WHERE);
                columnIndexes = appendColumns(sql, iterator);
                if (!ids.isEmpty()) {
                    sql.append(AND);
                }
            } else {
                columnIndexes = emptyMap();
                if (!ids.isEmpty()) {
                    sql.append(WHERE);
                }
            }
            if (!ids.isEmpty()) {
                sql.append(id)
                   .append(IN)
                   .append(nPlaceholders(ids.size()));
            }
            sql.append(SEMICOLON);

            collectQueryParameters(parameters, ids, columnIndexes);
            setQuery(sql.toString());
            return new SelectByEntityColumnsQuery<>(this);
        }

        private static Map<ColumnFilterIdentity, Integer> appendColumns(
                StringBuilder sql, Iterator<CompositeQueryParameter> parameters) {
            final Map<ColumnFilterIdentity, Integer> columnIndexes = newHashMap();
            int indexInStatement = 1;
            while (parameters.hasNext()) {
                final CompositeQueryParameter param = parameters.next();
                sql.append(BRACKET_OPEN);
                indexInStatement = appendSingleParameter(sql,
                                                         param,
                                                         columnIndexes,
                                                         indexInStatement);
                sql.append(BRACKET_CLOSE);
                if (parameters.hasNext()) {
                    sql.append(AND);
                }
            }
            return columnIndexes;
        }

        private static int appendSingleParameter(StringBuilder sql,
                                                 CompositeQueryParameter parameter,
                                                 Map<ColumnFilterIdentity, Integer> indexRegistry,
                                                 int indexInStatement) {
            final CompositeOperator operator = parameter.getOperator();
            final String compositeSqlOperator = toSql(operator);
            final UnmodifiableIterator<Map.Entry<EntityColumn, ColumnFilter>> filters =
                    parameter.getFilters()
                             .entries()
                             .iterator();
            int index = indexInStatement;
            while (filters.hasNext()) {
                Map.Entry<EntityColumn, ColumnFilter> filter = filters.next();
                final EntityColumn column = filter.getKey();
                indexRegistry.put(new ColumnFilterIdentity(column, filter.getValue(), parameter),
                                  index);
                index++;
                final String name = column.getStoredName();
                final Operator columnFilterOperator = filter.getValue()
                                                            .getOperator();
                final String comparisonOperator = toSql(columnFilterOperator);
                sql.append(name)
                   .append(comparisonOperator)
                   .append(PLACEHOLDER);
                if (filters.hasNext()) {
                    sql.append(compositeSqlOperator);
                }
            }
            return index;
        }

        private void collectQueryParameters(Iterable<CompositeQueryParameter> parameters,
                                            Iterable<I> ids,
                                            Map<ColumnFilterIdentity, Integer> indexRegistry) {
            collectParametersFromColumns(parameters, indexRegistry);
            int sqlParameterIndex = indexRegistry.size() + 1;
            collectParametersFromIds(ids, sqlParameterIndex);
        }

        @SuppressWarnings("MethodWithMultipleLoops") // OK in this case.
        private void collectParametersFromColumns(Iterable<CompositeQueryParameter> parameters,
                                                  Map<ColumnFilterIdentity, Integer> indexRegistry) {
            for (CompositeQueryParameter param : parameters) {
                for (Map.Entry<EntityColumn, ColumnFilter> filter : param.getFilters()
                                                                         .entries()) {
                    final EntityColumn column = filter.getKey();
                    final ColumnFilter columnFilter = filter.getValue();
                    final ColumnFilterIdentity filterId = new ColumnFilterIdentity(column,
                                                                                   columnFilter,
                                                                                   param);
                    final int columnIndexInSql = indexRegistry.get(filterId);
                    final JdbcColumnType<? super Object, ? super Object> columnType =
                            columnTypeRegistry.get(column);
                    final Object javaType = toObject(columnFilter.getValue(), column.getType());
                    final Object storedType = columnType.convertColumnValue(javaType);
                    columnType.setColumnValue(parametersBuilder, storedType, columnIndexInSql);
                }
            }
        }

        private void collectParametersFromIds(Iterable<I> ids, int startIndex) {
            int sqlParameterIndex = startIndex;
            final IdColumn<I, ?> idColumn = getIdColumn();
            for (I id : ids) {
                idColumn.setId(sqlParameterIndex, id, parametersBuilder);
                sqlParameterIndex++;
            }
        }

        @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
        private static String toSql(CompositeOperator operator) {
            checkArgument(operator.getNumber() > 0, operator.name());
            switch (operator) {
                case EITHER:
                    return OR.toString();
                case ALL:
                    return AND.toString();
                default:
                    throw newIllegalArgumentException("Unexpected composite operator %s.",
                                                      operator);
            }
        }

        @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
        private static String toSql(Operator operator) {
            checkArgument(operator.getNumber() > 0, operator.name());
            switch (operator) {
                case EQUAL:
                    return EQUAL.toString();
                case GREATER_THAN:
                    return GREATER_THAN.toString();
                case LESS_THAN:
                    return LESS_THAN.toString();
                case GREATER_OR_EQUAL:
                    return GREATER_OR_EQUAL.toString();
                case LESS_OR_EQUAL:
                    return LESS_OR_EQUAL.toString();
                default:
                    throw newIllegalArgumentException("Unexpected operator %s.",
                                                      operator);
            }
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }

    private static final class ColumnFilterIdentity {

        private final EntityColumn column;
        private final ColumnFilter filter;
        private final CompositeQueryParameter containingParameter;

        private ColumnFilterIdentity(EntityColumn column, ColumnFilter filter,
                                     CompositeQueryParameter containingParameter) {
            this.column = column;
            this.filter = filter;
            this.containingParameter = containingParameter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnFilterIdentity that = (ColumnFilterIdentity) o;
            return Objects.equal(column, that.column) &&
                   Objects.equal(filter, that.filter) &&
                   Objects.equal(containingParameter, that.containingParameter);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(column, filter, containingParameter);
        }
    }
}
