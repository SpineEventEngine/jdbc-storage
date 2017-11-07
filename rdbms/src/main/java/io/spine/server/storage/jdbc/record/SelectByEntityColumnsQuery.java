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

package io.spine.server.storage.jdbc.record;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.FieldMask;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparablePath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.client.ColumnFilter;
import io.spine.client.CompositeColumnFilter.CompositeOperator;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.CompositeQueryParameter;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.QueryParameters;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.querydsl.core.types.ExpressionUtils.and;
import static com.querydsl.core.types.ExpressionUtils.or;
import static com.querydsl.core.types.dsl.Expressions.TRUE;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static io.spine.protobuf.TypeConverter.toObject;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.entity;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * A query selecting the records from the {@link RecordTable RecordTable} by an {@link EntityQuery}.
 *
 * @author Dmytro Grankin
 */
final class SelectByEntityColumnsQuery<I> extends AbstractQuery
        implements SelectQuery<Iterator<EntityRecord>> {

    private final EntityQuery<I> entityQuery;
    private final FieldMask fieldMask;
    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;
    private final IdColumn<I> idColumn;

    private SelectByEntityColumnsQuery(Builder<I> builder) {
        super(builder);
        this.entityQuery = builder.entityQuery;
        this.fieldMask = builder.fieldMask;
        this.columnTypeRegistry = builder.columnTypeRegistry;
        this.idColumn = builder.idColumn;
    }

    @Override
    public Iterator<EntityRecord> execute() {
        final AbstractSQLQuery<Object, ?> query = factory().select(pathOf(entity))
                                                           .where(inIds())
                                                           .where(matchColumnValues())
                                                           .from(table());
        final ResultSet resultSet = query.getResults();
        return QueryResults.parse(resultSet, fieldMask);
    }

    /**
     * Obtains a predicate to match records by {@linkplain EntityQuery#getIds() IDs}.
     *
     * @return the predicate for IDs
     */
    private Predicate inIds() {
        final Set<I> ids = entityQuery.getIds();
        if (ids.isEmpty()) {
            return TRUE;
        }

        final PathBuilder<Object> id = pathOf(idColumn.getColumnName());
        final Collection<Object> normalizedIds = idColumn.normalize(ids);
        return id.in(normalizedIds);
    }

    /**
     * Obtains a predicate to match records by entity columns.
     *
     * @return the predicate for columns
     */
    private Predicate matchColumnValues() {
        BooleanExpression result = TRUE;
        final QueryParameters parameters = entityQuery.getParameters();
        for (CompositeQueryParameter parameter : parameters) {
            result = result.and(predicateFrom(parameter));
        }
        return result;
    }

    private Predicate predicateFrom(CompositeQueryParameter parameter) {
        Predicate result = TRUE;
        for (Map.Entry<EntityColumn, ColumnFilter> columnWithFilter : parameter.getFilters()
                                                                               .entries()) {
            final Predicate predicate = columnMatchFilter(columnWithFilter.getKey(),
                                                          columnWithFilter.getValue());
            result = joinPredicates(result, predicate, parameter.getOperator());
        }
        return result;
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
    @VisibleForTesting
    static Predicate joinPredicates(Predicate left,
                                    Predicate right,
                                    CompositeOperator operator) {
        checkArgument(operator.getNumber() > 0, operator.name());
        switch (operator) {
            case EITHER:
                return or(left, right);
            case ALL:
                return and(left, right);
            default:
                throw newIllegalArgumentException("Unexpected composite operator %s.",
                                                  operator);
        }
    }

    @SuppressWarnings("EnumSwitchStatementWhichMissesCases") // OK for the Protobuf enum switch.
    private Predicate columnMatchFilter(EntityColumn column, ColumnFilter filter) {
        final ColumnFilter.Operator operator = filter.getOperator();
        checkArgument(operator.getNumber() > 0, operator.name());

        final String columnName = column.getStoredName();
        final ComparablePath<Comparable> columnPath = comparablePath(Comparable.class, columnName);
        final JdbcColumnType<? super Object, ? super Object> columnType =
                columnTypeRegistry.get(column);
        final Object javaType = toObject(filter.getValue(), column.getType());
        final Comparable columnValue = (Comparable) columnType.convertColumnValue(javaType);

        switch (operator) {
            case EQUAL:
                return columnPath.eq(columnValue);
            case GREATER_THAN:
                return columnPath.gt(columnValue);
            case LESS_THAN:
                return columnPath.lt(columnValue);
            case GREATER_OR_EQUAL:
                return columnPath.goe(columnValue);
            case LESS_OR_EQUAL:
                return columnPath.loe(columnValue);
            default:
                throw newIllegalArgumentException("Unexpected operator %s.", operator);
        }
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>,
                                                          SelectByEntityColumnsQuery<I>> {

        private EntityQuery<I> entityQuery;
        private FieldMask fieldMask;
        private ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;
        private IdColumn<I> idColumn;

        private Builder() {
            super();
        }

        Builder<I> setEntityQuery(EntityQuery<I> entityQuery) {
            this.entityQuery = checkNotNull(entityQuery);
            return this;
        }

        Builder<I> setFieldMask(FieldMask fieldMask) {
            this.fieldMask = checkNotNull(fieldMask);
            return this;
        }

        Builder<I> setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> registry) {
            this.columnTypeRegistry = checkNotNull(registry);
            return this;
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = checkNotNull(idColumn);
            return this;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Checks that all the builder fields were set to a non-{@code null} values.
         */
        @Override
        protected void checkPreconditions() throws IllegalStateException {
            super.checkPreconditions();
            checkState(idColumn != null, "IdColumn is not set.");
            checkState(fieldMask != null, "FieldMask is not set.");
            checkState(entityQuery != null, "EntityQuery is not set.");
            checkState(columnTypeRegistry != null, "ColumnTypeRegistry is not set.");
        }

        @Override
        protected SelectByEntityColumnsQuery<I> doBuild() {
            return new SelectByEntityColumnsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
