/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.common.base.Functions;
import com.querydsl.core.dml.StoreClause;
import io.spine.server.entity.storage.ColumnRecords;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.Parameters;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static io.spine.server.storage.jdbc.query.Serializer.serialize;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ENTITY;

/**
 * An abstract base for write queries to the {@link RecordTable}.
 *
 * <p>The query can be used to work with {@linkplain Builder#addRecords(Map) multiple records}.
 *
 * <p>An overhead for multiple records will be the same as for a single record.
 *
 * @param <I> the type of IDs
 * @param <C> the type of {@link StoreClause}
 * @author Dmytro Grankin
 */
abstract class WriteEntityQuery<I, C extends StoreClause<C>> extends AbstractQuery implements WriteQuery {

    private final IdColumn<I> idColumn;
    private final Map<I, EntityRecordWithColumns> records;
    private final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;

    WriteEntityQuery(Builder<? extends Builder, ? extends WriteEntityQuery, I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.records = builder.records;
        this.columnTypeRegistry = builder.columnTypeRegistry;
    }

    @Override
    public long execute() {
        final C clause = createClause();
        for (I id : records.keySet()) {
            final EntityRecordWithColumns record = records.get(id);
            setEntityColumns(clause, record);

            clause.set(pathOf(ENTITY), serialize(record.getRecord()));
            setIdValue(clause, idColumn, idColumn.normalize(id));
            addBatch(clause);
        }
        return clause.execute();
    }

    /**
     * Adds a batch for the clause.
     *
     * <p>A batch is formed from the state of the clause.
     *
     * <p>After a batch was added, the clause can be used to prepare and add the next batch.
     *
     * @param clause the clause to add a batch for
     */
    protected abstract void addBatch(C clause);

    /**
     * Sets an ID value to the specified {@linkplain StoreClause clause}.
     *
     * @param clause       the clause to set ID
     * @param idColumn     the {@link IdColumn} representing the ID
     * @param normalizedId the {@linkplain IdColumn#normalize(Object) ID value} to set
     */
    protected abstract void setIdValue(C clause, IdColumn<I> idColumn, Object normalizedId);

    /**
     * Creates an empty {@linkplain StoreClause clause} representing this query.
     *
     * @return the new clause
     */
    protected abstract C createClause();

    private void setEntityColumns(C clause, EntityRecordWithColumns record) {
        final Parameters parameters = createParametersFromColumns(record);
        final Set<String> identifiers = parameters.getIdentifiers();
        for (String identifier : identifiers) {
            final Object parameterValue = parameters.getParameter(identifier)
                                                    .getValue();
            clause.set(pathOf(identifier), parameterValue);
        }
    }

    /**
     * Creates {@link Parameters} from
     * {@linkplain EntityRecordWithColumns#hasColumns() entity columns}.
     *
     * @param record the record to extract entity column values
     * @return query parameters from entity columns
     */
    private Parameters createParametersFromColumns(EntityRecordWithColumns record) {
        final Parameters.Builder parameters = Parameters.newBuilder();
        if (record.hasColumns()) {
            ColumnRecords.feedColumnsTo(parameters,
                                        record,
                                        columnTypeRegistry,
                                        Functions.<String>identity());
        }
        return parameters.build();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q, I>,
                                  Q extends WriteEntityQuery,
                                  I>
            extends AbstractQuery.Builder<B, Q> {

        private IdColumn<I> idColumn;
        private final Map<I, EntityRecordWithColumns> records = newLinkedHashMap();
        private ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry
                = JdbcTypeRegistryFactory.defaultInstance();

        B setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry) {
            this.columnTypeRegistry = columnTypeRegistry;
            return getThis();
        }

        B addRecord(I id, EntityRecordWithColumns record) {
            records.put(id, record);
            return getThis();
        }

        B addRecords(Map<I, EntityRecordWithColumns> records) {
            this.records.putAll(records);
            return getThis();
        }

        B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
