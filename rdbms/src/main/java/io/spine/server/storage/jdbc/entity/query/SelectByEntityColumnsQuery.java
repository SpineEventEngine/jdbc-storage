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

import com.google.common.base.Objects;
import com.google.protobuf.FieldMask;
import io.spine.client.ColumnFilter;
import io.spine.client.ColumnFilter.Operator;
import io.spine.client.CompositeColumnFilter.CompositeOperator;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.StorageQuery;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.spine.protobuf.TypeConverter.toObject;
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
import static io.spine.server.storage.jdbc.table.entity.RecordTable.StandardColumn.entity;
import static io.spine.server.storage.jdbc.table.entity.RecordTable.StandardColumn.id;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * @author Dmytro Dashenkov
 */
public final class SelectByEntityColumnsQuery<I> extends StorageQuery implements AutoCloseable {

    private static final String COMMON_SQL = SELECT.toString() + entity + FROM + "%s ";

    private final PreparedStatement statement;
    private final FieldMask fieldMask;

    private SelectByEntityColumnsQuery(Builder<I> builder) {
        super(builder);
        this.statement = builder.getStatement();
        this.fieldMask = builder.getFieldMask();
    }

    public Iterator<EntityRecord> execute() {
        try (PreparedStatement statement = this.statement) {
            final ResultSet resultSet = statement.executeQuery();
            return QueryResults.parse(resultSet, fieldMask);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        return statement;
    }

    /**
     * Closes the underlying {@link PreparedStatement} unless {@link #execute()} has been called.
     *
     * @throws SQLException from {@link PreparedStatement#close() PreparedStatement.close()}
     */
    @Override
    public void close() throws SQLException {
        statement.close();
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<I> extends StorageQuery.Builder<Builder<I>,
                                                                SelectByEntityColumnsQuery<I>> {

        private EntityQuery<I> entityQuery;
        private FieldMask fieldMask;
        private ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;
        private IdColumn<I> idColumn;
        private String tableName;

        private PreparedStatement statement;

        private Builder() {
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

        public Builder<I> setColumnTypeRegistry(ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> registry) {
            this.columnTypeRegistry = checkNotNull(registry);
            return this;
        }

        public Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return this;
        }

        public Builder<I> setTableName(String tableName) {
            this.tableName = checkNotNull(tableName);
            return this;
        }

        private PreparedStatement getStatement() {
            return checkNotNull(statement);
        }

        private IdColumn<I> getIdColumn() {
            return checkNotNull(idColumn);
        }

        private String getTableName() {
            return checkNotNull(tableName);
        }

        @Override
        public SelectByEntityColumnsQuery<I> build() {
            // TODO:2017-07-22:dmytro.dashenkov: Simplify.

            checkState(entityQuery != null, "EntityQuery is not set.");
            checkState(columnTypeRegistry != null, "ColumnTypeRegistry is not set.");

            final StringBuilder sql = new StringBuilder(String.format(COMMON_SQL, getTableName()));
            final QueryParameters parameters = entityQuery.getParameters();
            final Collection<I> ids = entityQuery.getIds();
            final Iterator<CompositeQueryParameter> iterator = parameters.iterator();
            final Map<ColumnFilterIdentity, Integer> columnIndexes = new HashMap<>();
            if (iterator.hasNext()) {
                sql.append(WHERE);
                int indexInStatement = 1;
                while (iterator.hasNext()) {
                    final CompositeQueryParameter param = iterator.next();
                    final CompositeOperator operator = param.getOperator();
                    sql.append(BRACKET_OPEN);
                    final String compositeSqlOperator = toSql(operator);
                    final Iterator<Map.Entry<Column, ColumnFilter>> filters = param.getFilters()
                                                                                   .entries()
                                                                                   .iterator();
                    while (filters.hasNext()) {
                        Map.Entry<Column, ColumnFilter> filter = filters.next();
                        final Column column = filter.getKey();
                        columnIndexes.put(new ColumnFilterIdentity(column, param),
                                          indexInStatement);
                        indexInStatement++;
                        final String name = column.getName();
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
                    sql.append(BRACKET_CLOSE);
                    if (iterator.hasNext()) {
                        sql.append(AND);
                    }
                }
                if (!ids.isEmpty()) {
                    sql.append(AND);
                }
            } else {
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
            final DataSourceWrapper dataSource = getDataSource();
            final ConnectionWrapper connection = dataSource.getConnection(true);
            this.statement = connection.prepareStatement(sql.toString());

            for (CompositeQueryParameter param : parameters) {
                for (Map.Entry<Column, ColumnFilter> filter : param.getFilters().entries()) {
                    final Column column = filter.getKey();
                    final ColumnFilter columnFilter = filter.getValue();
                    final int columnIndexInSql = columnIndexes.get(new ColumnFilterIdentity(filter.getKey(), param));
                    final JdbcColumnType<? super Object, ? super Object> columnType = columnTypeRegistry.get(column);
                    final Object javaType = toObject(columnFilter.getValue(), column.getType());
                    final Object storedType = columnType.convertColumnValue(javaType);
                    columnType.setColumnValue(statement, storedType, columnIndexInSql);
                }
            }

            int sqlParameterIndex = columnIndexes.size() + 1;
            final IdColumn<I> idColumn = getIdColumn();
            for (I id : ids) {
                idColumn.setId(sqlParameterIndex, id, statement);
                sqlParameterIndex++;
            }

            return new SelectByEntityColumnsQuery<>(this);
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

        private final Column column;
        private final CompositeQueryParameter containingParameter;

        private ColumnFilterIdentity(Column column, CompositeQueryParameter parameter) {
            this.column = column;
            this.containingParameter = parameter;
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
                   Objects.equal(containingParameter, that.containingParameter);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(column, containingParameter);
        }
    }
}
